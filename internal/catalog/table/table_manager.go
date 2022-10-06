package table

import (
	"fmt"
	"go-db/internal/buffer"
	"go-db/internal/catalog/column"
	"go-db/internal/catalog/schema"
	"go-db/internal/catalog/tuple"
	"go-db/internal/common/constant"
	"go-db/internal/common/errors"
	"go-db/internal/common/types"
	"log"
	"sync"
)

//TODO should to implement failure rollback
type TableManager struct {
	bufferPoolManager *buffer.BufferPoolManager
	TableMetaPageID   map[string]types.Page_id_t
	RLock             sync.RWMutex
}

func NewTableManager(bufferPoolManager *buffer.BufferPoolManager, tableMetaPageID map[string]types.Page_id_t) *TableManager {
	return &TableManager{
		bufferPoolManager: bufferPoolManager,
		TableMetaPageID:   tableMetaPageID,
	}
}

func (t *TableManager) GetTables() []string {
	tableName := make([]string, 0, len(t.TableMetaPageID))

	for k := range t.TableMetaPageID {
		tableName = append(tableName, k)
	}

	return tableName
}

func (t *TableManager) GetTableMeta(tableName string) ([]*column.Column, error) {
	pageID, err := t.getMetaPageID(tableName)

	if err != nil {
		return nil, err
	}

	page, err := t.bufferPoolManager.FetchPage(pageID)

	if err != nil {
		return nil, err
	}
	return schema.GetSchema(page).GetColumns(), nil
}

func (t *TableManager) CreateNewTable(tableName string, columns []*column.Column) error {
	newPage, err := t.bufferPoolManager.NewPage()

	if err != nil {
		log.Println(err)
		return err
	}

	metaPage := schema.GetSchema(newPage)
	defer t.bufferPoolManager.FlushPage(metaPage.GetPageID())

	// if any error occur should have the method to rollback
	for _, c := range columns {
		metaPage.AddColumn(c)
	}

	metaPage.SetTableName(tableName)

	t.RLock.Lock()
	t.TableMetaPageID[tableName] = metaPage.GetPageID()
	t.RLock.Unlock()

	dataPage, err := t.bufferPoolManager.NewPage()

	if err != nil {
		log.Println(err)
		return err
	}

	metaPage.SetDataPageID(dataPage.GetPageID())
	GetDataTable(dataPage).DataTableInit()
	t.bufferPoolManager.FlushPage(dataPage.GetPageID())
	t.bufferPoolManager.FlushPage(metaPage.GetPageID())

	return nil
}

func (t *TableManager) AddNewColumn(tableName string, column *column.Column) error {
	pageID, err := t.getMetaPageID(tableName)

	if err != nil {
		return err
	}

	page, err := t.bufferPoolManager.FetchPage(pageID)

	if err != nil {
		return err
	}

	metaPage := schema.GetSchema(page)
	defer t.bufferPoolManager.FlushPage(metaPage.GetPageID())

	metaPage.AddColumn(column)

	return nil
}

func (t *TableManager) InsertTuple(tableName string, value []*tuple.Value) error {
	metaTablePageID, exist := t.TableMetaPageID[tableName]

	if !exist {
		return errors.ErrNoTable
	}

	page, err := t.bufferPoolManager.FetchPage(metaTablePageID)

	if err != nil {
		return err
	}

	metaTable := schema.GetSchema(page)
	dataTablePageID := metaTable.GetDataPageID()

	tupleSize := t.getValueSize(value)

getPage:
	dataPage, err := t.bufferPoolManager.FetchPage(dataTablePageID)
	if err != nil {
		return err
	}
	dataTablePage := GetDataTable(dataPage)

	if dataTablePage.GetRemainSpace() < tupleSize {
		dataTablePageID = dataTablePage.GetNextPageID()
		if dataTablePageID == constant.INVALID_PAGE_ID {
			newDataPage, err := t.bufferPoolManager.NewPage()
			newDataPage.SetPageType(types.DATA_PAGE_TYPE)
			if err != nil {
				return err
			}

			dataTablePage.SetNextPageID(newDataPage.GetPageID())
			dataTablePageID = newDataPage.GetPageID()
			GetDataTable(newDataPage).DataTableInit()
		}
		goto getPage
	}

	if err := dataTablePage.InsertTuple(value, tupleSize); err != nil {
		return err
	}

	tempTuples := dataTablePage.GetTuple(metaTable)

	for _, t := range tempTuples {
		for _, a := range t {
			fmt.Println(*a)
		}
	}

	t.bufferPoolManager.UnpinPage(dataTablePageID)
	t.bufferPoolManager.FlushPage(dataTablePageID)
	return nil
}

func (t *TableManager) GetTuples(tableName string) ([][]*tuple.Value, error) {
	metaTablePageID, exist := t.TableMetaPageID[tableName]

	if !exist {
		return nil, errors.ErrNoTable
	}

	page, err := t.bufferPoolManager.FetchPage(metaTablePageID)

	if err != nil {
		return nil, err
	}

	metaTable := schema.GetSchema(page)
	dataTablePageID := metaTable.GetDataPageID()

	tuples := make([][]*tuple.Value, 0)

	for dataTablePageID != constant.INVALID_PAGE_ID {
		page, err = t.bufferPoolManager.FetchPage(dataTablePageID)

		if err != nil {
			return nil, err
		}

		dataTable := GetDataTable(page)

		tuples = append(tuples, dataTable.GetTuple(metaTable)...)

		dataTablePageID = dataTable.GetNextPageID()
	}

	return tuples, nil
}

func (t *TableManager) UpdateTuple(tableName string, tupleID int32, values []*tuple.Value) error {
	return nil
}

func (t *TableManager) getMetaPageID(tableName string) (types.Page_id_t, error) {
	t.RLock.RLock()
	defer t.RLock.RUnlock()
	pageID, exist := t.TableMetaPageID[tableName]

	if !exist {
		return 0, errors.ErrPageNotFound
	}

	return pageID, nil
}

func (t *TableManager) getValueSize(values []*tuple.Value) int32 {
	var size int32
	for _, v := range values {
		size += v.GetSize()
	}

	return size
}
