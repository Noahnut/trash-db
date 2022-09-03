package table

import (
	"go-db/internal/buffer"
	"go-db/internal/catalog/column"
	"go-db/internal/catalog/tuple"
	"go-db/internal/common/types"
	"go-db/internal/storage/disk"
	"go-db/internal/utils"
	"log"
	"strings"
	"testing"
)

func Test_CreateNewTable(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	tableName := "testTable"

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableManager := NewTableManager(bufferPool, map[string]types.Page_id_t{})

	columns := []*column.Column{
		{
			ColumnType: types.BOOL_TYPE,
			Name:       "bool_type",
		}, {
			ColumnType: types.FLOAT_TYPE,
			Name:       "float_type",
		}, {
			ColumnType: types.INT_TYPE,
			Name:       "int_types",
		}, {
			ColumnType: types.LONG_INT_TYPE,
			Name:       "long_int_type",
		}, {
			ColumnType: types.VAR_CHAR_TYPE,
			Name:       "var_char_type",
		},
	}

	tableManager.CreateNewTable(tableName, columns)

	testColumns, err := tableManager.GetTableMeta(tableName)

	if err != nil {
		t.Fatal(err)
	}

	for i, testColumn := range testColumns {
		if testColumn.ColumnType != columns[i].ColumnType || testColumn.Name != columns[i].Name {
			t.Error("get column wrong")
		}
	}

	tables := tableManager.GetTables()

	if len(tables) != 1 || tables[0] != tableName {
		t.Error("table name wrong")
	}
}

func Test_CreateNewTables(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	tableHash := map[string]struct{}{
		"testTable":      {},
		"testTableTwo":   {},
		"testTableThree": {},
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableManager := NewTableManager(bufferPool, map[string]types.Page_id_t{})

	columns := []*column.Column{
		{
			ColumnType: types.BOOL_TYPE,
			Name:       "bool_type",
		}, {
			ColumnType: types.FLOAT_TYPE,
			Name:       "float_type",
		}, {
			ColumnType: types.INT_TYPE,
			Name:       "int_types",
		}, {
			ColumnType: types.LONG_INT_TYPE,
			Name:       "long_int_type",
		}, {
			ColumnType: types.VAR_CHAR_TYPE,
			Name:       "var_char_type",
		},
	}

	for k := range tableHash {
		tableManager.CreateNewTable(k, columns)
	}

	tables := tableManager.GetTables()

	for _, table := range tables {
		if _, exist := tableHash[table]; !exist {
			t.Error("get tables wrong")
		}
	}

	for _, table := range tables {
		column, err := tableManager.GetTableMeta(table)

		if err != nil {
			t.Fatal(err)
		}

		for i, testColumn := range column {
			if testColumn.ColumnType != columns[i].ColumnType || testColumn.Name != columns[i].Name {
				t.Error("get column wrong")
			}
		}
	}
}

func Test_AddNewColumn(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	tableName := "testTable"

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableManager := NewTableManager(bufferPool, map[string]types.Page_id_t{})

	columns := []*column.Column{
		{
			ColumnType: types.BOOL_TYPE,
			Name:       "bool_type",
		}, {
			ColumnType: types.FLOAT_TYPE,
			Name:       "float_type",
		}, {
			ColumnType: types.INT_TYPE,
			Name:       "int_types",
		}, {
			ColumnType: types.LONG_INT_TYPE,
			Name:       "long_int_type",
		}, {
			ColumnType: types.VAR_CHAR_TYPE,
			Name:       "var_char_type",
		},
	}

	tableManager.CreateNewTable(tableName, columns)

	testColumns, err := tableManager.GetTableMeta(tableName)

	if err != nil {
		t.Fatal(err)
	}

	for i, testColumn := range testColumns {
		if testColumn.ColumnType != columns[i].ColumnType || testColumn.Name != columns[i].Name {
			t.Error("get column wrong")
		}
	}

	newColumn := &column.Column{
		ColumnType: types.BOOL_TYPE,
		Name:       "new_bool_type",
	}

	columns = append(columns, newColumn)

	tableManager.AddNewColumn(tableName, newColumn)

	testColumns, err = tableManager.GetTableMeta(tableName)

	if err != nil {
		t.Fatal(err)
	}

	for i, testColumn := range testColumns {
		if testColumn.ColumnType != columns[i].ColumnType || testColumn.Name != columns[i].Name {
			t.Error("get column wrong")
		}
	}
}

func Test_TableManagerInsertTuple(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	tableName := "testTable"

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableManager := NewTableManager(bufferPool, map[string]types.Page_id_t{})

	columns := make([]*column.Column, 0)

	columns = append(columns, column.NewColumn(types.BOOL_TYPE, 0, "bool_type"))
	columns = append(columns, column.NewColumn(types.FLOAT_TYPE, 0, "float_type"))
	columns = append(columns, column.NewColumn(types.INT_TYPE, 0, "int_types"))
	columns = append(columns, column.NewColumn(types.LONG_INT_TYPE, 0, "long_int_type"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type"))

	tableManager.CreateNewTable(tableName, columns)

	tuples := make([]*tuple.Value, 0)

	boolType := true
	floatType := 1.0
	var interType int32 = 4
	var longInt int64 = 5
	varCharType := []byte("123")

	tuples = append(tuples, tuple.GetValue(boolType, columns[0].GetColumnType(), columns[0].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(floatType, columns[1].GetColumnType(), columns[1].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(interType, columns[2].GetColumnType(), columns[2].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(longInt, columns[3].GetColumnType(), columns[3].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(varCharType, columns[4].GetColumnType(), columns[4].GetColumnSize()))

	err = tableManager.InsertTuple(tableName, tuples)

	if err != nil {
		t.Fatal(err)
	}

	getTuples, err := tableManager.GetTuples(tableName)

	if err != nil {
		t.Fatal(err)
	}

	if len(getTuples) != 1 {
		t.Error("get Tuple not one")
	}

	for i, tuple := range getTuples[0] {
		if tuples[i].GetType() != tuple.GetType() {
			t.Error("return tuple type wrong ")
		}

		switch tuple.GetType() {
		case types.BOOL_TYPE:
			if tuple.BOOL != tuples[i].BOOL {
				t.Error("bool data wrong")
			}
		case types.FLOAT_TYPE:
			if tuple.FLOAT != tuples[i].FLOAT {
				t.Error("float data wrong")
			}
		case types.LONG_INT_TYPE:
			if tuple.LONG_INT != tuples[i].LONG_INT {
				t.Error("long int data wrong")
			}
		case types.INT_TYPE:
			if tuple.INT != tuples[i].INT {
				t.Error("int data wrong")
			}
		case types.VAR_CHAR_TYPE:
			if strings.Compare(utils.ConvertByteToString(tuple.VAR_CHAR), utils.ConvertByteToString(tuples[i].VAR_CHAR)) != 0 {
				t.Error("var char data wrong")
			}
		}
	}

}

func Test_TableManagerInsertTupleFullPage(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	tableName := "testTable"

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableManager := NewTableManager(bufferPool, map[string]types.Page_id_t{})

	columns := make([]*column.Column, 0)

	columns = append(columns, column.NewColumn(types.BOOL_TYPE, 0, "bool_type"))
	columns = append(columns, column.NewColumn(types.FLOAT_TYPE, 0, "float_type"))
	columns = append(columns, column.NewColumn(types.INT_TYPE, 0, "int_types"))
	columns = append(columns, column.NewColumn(types.LONG_INT_TYPE, 0, "long_int_type"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type"))

	tableManager.CreateNewTable(tableName, columns)

	tuples := make([]*tuple.Value, 0)

	boolType := true
	floatType := 1.0
	var interType int32 = 4
	var longInt int64 = 5
	varCharType := []byte("123")

	tuples = append(tuples, tuple.GetValue(boolType, columns[0].GetColumnType(), columns[0].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(floatType, columns[1].GetColumnType(), columns[1].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(interType, columns[2].GetColumnType(), columns[2].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(longInt, columns[3].GetColumnType(), columns[3].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(varCharType, columns[4].GetColumnType(), columns[4].GetColumnSize()))

	for i := 0; i < 1000; i++ {
		tableManager.InsertTuple(tableName, tuples)
	}

	getTuples, err := tableManager.GetTuples(tableName)

	if err != nil {
		t.Fatal(err)
	}

	if len(getTuples) != 1000 {
		t.Error("tuple number wrong")
	}

	for i := 0; i < 1000; i++ {
		for i, tuple := range getTuples[i] {
			if tuples[i].GetType() != tuple.GetType() {
				t.Error("return tuple type wrong ")
			}

			switch tuple.GetType() {
			case types.BOOL_TYPE:
				if tuple.BOOL != tuples[i].BOOL {
					t.Error("bool data wrong")
				}
			case types.FLOAT_TYPE:
				if tuple.FLOAT != tuples[i].FLOAT {
					t.Error("float data wrong")
				}
			case types.LONG_INT_TYPE:
				if tuple.LONG_INT != tuples[i].LONG_INT {
					t.Error("long int data wrong")
				}
			case types.INT_TYPE:
				if tuple.INT != tuples[i].INT {
					t.Error("int data wrong")
				}
			case types.VAR_CHAR_TYPE:
				if strings.Compare(utils.ConvertByteToString(tuple.VAR_CHAR), utils.ConvertByteToString(tuples[i].VAR_CHAR)) != 0 {
					t.Error("var char data wrong")
				}
			}
		}
	}

}
