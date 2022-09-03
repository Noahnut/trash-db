package schema

import (
	"encoding/binary"
	"go-db/internal/catalog/column"
	"go-db/internal/common/constant"
	"go-db/internal/common/errors"
	"go-db/internal/common/types"
	"go-db/internal/storage/page"
	"go-db/internal/utils"
	"sync"
)

/**
*  META_TABLE_TYPE
*  +------------+----------------+----------------+------------------+-----------------+----------------+
*  | PageType(4)| PrevPageID (4) |  NextPageID (4)| Data PageID (4)  | TABLE_NAME (256)|Column count (4)|
*  +------------+----------------+----------------+------------------+-----------------+----------------+
*  +-------------------+-----------------+-----------------------+
*  |Column 1 Name(128) | Column 1 Type(4)| Column 1 Size(4) ...  | ....
*  +-------------------+-----------------+-----------------------+
**/

type Schema struct {
	*page.Page
	RLock sync.RWMutex
}

func GetSchema(page *page.Page) *Schema {
	return &Schema{
		Page: page,
	}
}

func (m *Schema) GetTableName() string {
	m.RLock.RLock()
	defer m.RLock.RUnlock()
	return utils.ConvertByteToString(m.GetData()[types.DATA_PAGE_ID_OFFSET:types.TABLE_NAME_OFFSET])
}

func (m *Schema) SetTableName(tableName string) {
	m.RLock.Lock()
	defer m.RLock.Unlock()
	copy(m.GetData()[types.DATA_PAGE_ID_OFFSET:types.TABLE_NAME_OFFSET], []byte(tableName))
}

func (m *Schema) GetDataPageID() types.Page_id_t {
	m.RLock.RLock()
	defer m.RLock.RUnlock()
	return types.Page_id_t(binary.BigEndian.Uint32(m.GetData()[types.NEXT_PAGE_ID_OFFSET:types.DATA_PAGE_ID_OFFSET]))
}

func (m *Schema) SetDataPageID(pageID types.Page_id_t) {
	m.RLock.Lock()
	defer m.RLock.Unlock()
	binary.BigEndian.PutUint32(m.GetData()[types.NEXT_PAGE_ID_OFFSET:types.DATA_PAGE_ID_OFFSET], uint32(pageID))
}

func (m *Schema) GetColumnCount() int32 {
	m.RLock.RLock()
	defer m.RLock.RUnlock()
	return int32(binary.BigEndian.Uint32(m.GetData()[types.TABLE_NAME_OFFSET:types.COLUMN_COUNT]))
}

func (m *Schema) SetColumnCount(count int32) {
	m.RLock.Lock()
	defer m.RLock.Unlock()
	binary.BigEndian.PutUint32(m.GetData()[types.TABLE_NAME_OFFSET:types.COLUMN_COUNT], uint32(count))
}

func (m *Schema) AddColumn(column *column.Column) bool {
	columnCount := m.GetColumnCount()

	m.RLock.Lock()

	columnOffset := int32(types.COLUMN_COUNT) + ((types.COLUMN_SIZE_OFFSET + types.COLUMN_TYPE_OFFSET + types.COLUMN_NAME_OFFSET) * columnCount)

	if columnOffset+types.COLUMN_SIZE_OFFSET+types.COLUMN_TYPE_OFFSET+types.COLUMN_NAME_OFFSET > constant.PAGE_SIZE {
		return false
	}

	copy(m.GetData()[columnOffset:m.getColumnNameOffset(columnOffset)], []byte(column.Name))
	binary.BigEndian.PutUint32(m.GetData()[m.getColumnNameOffset(columnOffset):m.getColumnTypeOffset(columnOffset)], uint32(column.ColumnType))
	binary.BigEndian.PutUint32(m.GetData()[m.getColumnTypeOffset(columnOffset):m.getColumnSizeOffset(columnOffset)], uint32(column.Size))
	m.RLock.Unlock()
	m.SetColumnCount(columnCount + 1)

	return true
}

func (m *Schema) GetColumns() []*column.Column {
	m.RLock.RLock()
	defer m.RLock.RUnlock()

	columnCount := m.GetColumnCount()

	columns := make([]*column.Column, 0, columnCount)

	columnOffset := int32(types.COLUMN_COUNT)

	for i := 0; i < int(columnCount); i++ {
		c := &column.Column{
			Name:       utils.ConvertByteToString(m.GetData()[columnOffset:m.getColumnNameOffset(columnOffset)]),
			ColumnType: types.COLUMN_TYPE(binary.BigEndian.Uint32(m.GetData()[m.getColumnNameOffset(columnOffset):m.getColumnTypeOffset(columnOffset)])),
			Size:       int32(binary.BigEndian.Uint32(m.GetData()[m.getColumnTypeOffset(columnOffset):m.getColumnSizeOffset(columnOffset)])),
		}
		columnOffset += (types.COLUMN_NAME_OFFSET + types.COLUMN_TYPE_OFFSET + types.COLUMN_SIZE_OFFSET)
		columns = append(columns, c)
	}

	return columns
}

func (m *Schema) GetColumnByIndex(index int32) (*column.Column, error) {
	m.RLock.RLock()
	defer m.RLock.RUnlock()

	if m.GetColumnCount() < index {
		return nil, errors.ErrColumnIndexOutOfRange
	}

	columnOffset := int32(types.COLUMN_COUNT) + ((types.COLUMN_SIZE_OFFSET + types.COLUMN_TYPE_OFFSET + types.COLUMN_NAME_OFFSET) * index)

	c := &column.Column{
		Name:       utils.ConvertByteToString(m.GetData()[columnOffset:m.getColumnNameOffset(columnOffset)]),
		ColumnType: types.COLUMN_TYPE(binary.BigEndian.Uint32(m.GetData()[m.getColumnNameOffset(columnOffset):m.getColumnTypeOffset(columnOffset)])),
		Size:       int32(binary.BigEndian.Uint32(m.GetData()[m.getColumnTypeOffset(columnOffset):m.getColumnSizeOffset(columnOffset)])),
	}

	return c, nil
}

func (m *Schema) getColumnNameOffset(columnOffset int32) int32 {
	return columnOffset + types.COLUMN_NAME_OFFSET
}

func (m *Schema) getColumnTypeOffset(columnOffset int32) int32 {
	return columnOffset + types.COLUMN_NAME_OFFSET + types.COLUMN_TYPE_OFFSET
}

func (m *Schema) getColumnSizeOffset(columnOffset int32) int32 {
	return columnOffset + types.COLUMN_NAME_OFFSET + types.COLUMN_TYPE_OFFSET + types.COLUMN_SIZE_OFFSET
}
