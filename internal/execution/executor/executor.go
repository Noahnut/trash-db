package executor

import (
	"encoding/json"
	"fmt"
	"go-db/internal/buffer"
	"go-db/internal/catalog/column"
	"go-db/internal/catalog/table"
	"go-db/internal/catalog/tuple"
	"go-db/internal/common/errors"
	"go-db/internal/common/types"
	"go-db/internal/execution/ast"
	"go-db/internal/execution/parser"
	"go-db/internal/storage/disk"
	"strings"
)

type Executor struct {
	bufferPool   *buffer.BufferPoolManager
	diskManager  *disk.Disk
	tableManager *table.TableManager
}

func NewExecutor(bufferPool *buffer.BufferPoolManager, diskManager *disk.Disk, tableManager *table.TableManager) *Executor {
	return &Executor{
		bufferPool:   bufferPool,
		diskManager:  diskManager,
		tableManager: tableManager,
	}
}

func (e *Executor) QueryExecutor(query string) ([]byte, error) {
	ast, err := parser.ParseSQLQuery(query)

	var (
		response []byte
	)

	if err != nil {
		return nil, err
	}

	if ast.Type == types.SELECT_QUERY_TYPE {
		response, err = e.selectQueryExecutor(ast)

		if err != nil {
			return nil, err
		}
	} else if ast.Type == types.INSERT_QUERY_TYPE {
		response, err = e.insertQueryExecutor(ast)

		if err != nil {
			return nil, err
		}
	} else if ast.Type == types.CREATE_QUERY_TYPE {
		response, err = e.createQueryExecutor(ast)

		if err != nil {
			return nil, err
		}
	}

	return response, nil
}

func (e *Executor) selectQueryExecutor(ast *ast.Ast) ([]byte, error) {
	columns, err := e.tableManager.GetTableMeta(ast.Table)

	if err != nil {
		return nil, err
	}

	tuples, err := e.tableManager.GetTuples(ast.Table)

	if err != nil {
		return nil, err
	}

	if ast.Limit != 0 {
		tuples = tuples[:ast.Limit]
	}

	jsonMap := make(map[string][]interface{})

	for _, v := range ast.Column {
		if v == types.QUERY_CHAR_STAR {
			for _, c := range columns {
				jsonMap[c.Name] = make([]interface{}, 0)
			}
			break
		} else {
			if _, exist := jsonMap[v]; exist {
				continue
			}
			jsonMap[v] = make([]interface{}, 0)
		}
	}

	for _, t := range tuples {
		for i := 0; i < len(t); i++ {
			if _, exist := jsonMap[columns[i].Name]; exist {
				jsonMap[columns[i].Name] = append(jsonMap[columns[i].Name], tuple.GetValueInterface(t[i]))
			}
		}
	}

	response, err := json.Marshal(jsonMap)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *Executor) insertQueryExecutor(ast *ast.Ast) ([]byte, error) {
	columns, err := e.tableManager.GetTableMeta(ast.Table)

	if err != nil {
		return nil, err
	}

	columMap := make(map[string]int)
	values := make([]*tuple.Value, len(columns))

	for i, c := range columns {
		columMap[c.Name] = i
		values[i] = tuple.GetValue(tuple.GetDefaultValue(c.ColumnType), c.GetColumnType(), c.GetColumnSize())
	}

	for i, value := range ast.Value {
		if _, exist := columMap[ast.Column[i]]; !exist {
			return nil, errors.ErrColumnNotExist
		}
		values[columMap[ast.Column[i]]] = tuple.GetValue(value, columns[columMap[ast.Column[i]]].GetColumnType(), columns[columMap[ast.Column[i]]].GetColumnSize())
	}

	err = e.tableManager.InsertTuple(ast.Table, values)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (e *Executor) createQueryExecutor(ast *ast.Ast) ([]byte, error) {
	tableColumns := make([]*column.Column, len(ast.Column))

	for i, col := range ast.Column {
		typeSize, columnType := int32(0), types.INVALID_TYPE

		switch ast.ColumnType[i] {
		case types.COLUMN_TYPE_BOOL:
			columnType = types.BOOL_TYPE
		case types.COLUMN_TYPE_FLOAT:
			columnType = types.FLOAT_TYPE
		case types.COLUMN_TYPE_INT:
			columnType = types.INT_TYPE
		case types.COLUMN_TYPE_LONGINT:
			columnType = types.LONG_INT_TYPE
		}

		if columnType == types.INVALID_TYPE {
			if strings.HasPrefix(ast.ColumnType[i], types.COLUMN_TYPE_VAR_CHAR) {
				columnType = types.VAR_CHAR_TYPE
				fmt.Sscanf(ast.ColumnType[i], types.COLUMN_TYPE_VAR_CHAR+"(%d)", &typeSize)
			}
		}

		tableColumns[i] = column.NewColumn(columnType, typeSize, col)
	}

	err := e.tableManager.CreateNewTable(ast.Table, tableColumns)

	if err != nil {
		return nil, err
	}

	return nil, nil
}
