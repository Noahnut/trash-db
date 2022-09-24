package executor

import (
	"go-db/internal/buffer"
	"go-db/internal/catalog/column"
	"go-db/internal/catalog/table"
	"go-db/internal/catalog/tuple"
	"go-db/internal/common/types"
	"go-db/internal/storage/disk"
	"log"
	"reflect"
	"testing"
)

func Test_SelectExecutor(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableName := "tableTest"

	tableManager := table.NewTableManager(bufferPool, make(map[string]types.Page_id_t))

	newPage, err := bufferPool.NewPage()

	if err != nil {
		t.Fatal(err)
	}

	dataPage := table.GetDataTable(newPage)
	dataPage.DataTableInit()

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

	valueSize := 0

	for _, t := range tuples {
		valueSize += int(t.GetSize())
	}

	tableManager.InsertTuple("tableTest", tuples)

	if err != nil {
		t.Fatal(err)
	}

	executor := NewExecutor(bufferPool, diskManager, tableManager)

	result, err := executor.QueryExecutor("select * from tableTest")

	if err != nil {
		t.Fatal(err)
	}

	expectResult := `{"bool_type":[true],"float_type":[1],"int_types":[4],"long_int_type":[5],"var_char_type":["123"]}`

	if string(result) != string(expectResult) {
		t.Error("get the wrong response", string(result))
	}

	tuples = make([]*tuple.Value, 0)

	boolType = false
	floatType = 2.2
	interType = 777777
	longInt = 88888
	varCharType = []byte("abcd")

	tuples = append(tuples, tuple.GetValue(boolType, columns[0].GetColumnType(), columns[0].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(floatType, columns[1].GetColumnType(), columns[1].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(interType, columns[2].GetColumnType(), columns[2].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(longInt, columns[3].GetColumnType(), columns[3].GetColumnSize()))
	tuples = append(tuples, tuple.GetValue(varCharType, columns[4].GetColumnType(), columns[4].GetColumnSize()))

	valueSize = 0

	for _, t := range tuples {
		valueSize += int(t.GetSize())
	}

	tableManager.InsertTuple("tableTest", tuples)

	if err != nil {
		t.Fatal(err)
	}

	result, err = executor.QueryExecutor("select * from tableTest")

	if err != nil {
		t.Fatal(err)
	}

	expectResult = `{"bool_type":[true,false],"float_type":[1,2.2],"int_types":[4,777777],"long_int_type":[5,88888],"var_char_type":["123","abcd"]}`

	if expectResult != string(result) {
		t.Error("get the wrong response", string(result))
	}

	result, err = executor.QueryExecutor("select bool_type, int_types from tableTest")

	if err != nil {
		t.Fatal(err)
	}

	expectResult = `{"bool_type":[true,false],"int_types":[4,777777]}`

	if expectResult != string(result) {
		t.Error("get the wrong response")
	}

	result, err = executor.QueryExecutor("select bool_type, int_types from tableTest limit 1")

	if err != nil {
		t.Fatal(err)
	}

	expectResult = `{"bool_type":[true],"int_types":[4]}`

	if expectResult != string(result) {
		t.Error("get the wrong response")
	}
}

func Test_InsertExecutor(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableName := "tableTest"

	tableManager := table.NewTableManager(bufferPool, make(map[string]types.Page_id_t))

	columns := make([]*column.Column, 0)

	columns = append(columns, column.NewColumn(types.BOOL_TYPE, 0, "bool_type"))
	columns = append(columns, column.NewColumn(types.FLOAT_TYPE, 0, "float_type"))
	columns = append(columns, column.NewColumn(types.INT_TYPE, 0, "int_types"))
	columns = append(columns, column.NewColumn(types.LONG_INT_TYPE, 0, "long_int_type"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type"))

	tableManager.CreateNewTable(tableName, columns)

	executor := NewExecutor(bufferPool, diskManager, tableManager)

	_, err = executor.QueryExecutor(`INSERT INTO tableTest (bool_type, float_type, int_types,long_int_type,var_char_type) VALUES (true, 0.1, 1, 1000, "test")`)

	if err != nil {
		t.Fatal(err)
	}

	result, err := executor.QueryExecutor("SELECT * FROM tableTest")

	if err != nil {
		t.Fatal(err)
	}

	expectResult := `{"bool_type":[true],"float_type":[0.1],"int_types":[1],"long_int_type":[1000],"var_char_type":["\"test\""]}`

	if string(result) != expectResult {
		t.Error("get wrong response", string(result))
	}
}

func Test_CreateExecutor(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	tableManager := table.NewTableManager(bufferPool, make(map[string]types.Page_id_t))

	executor := NewExecutor(bufferPool, diskManager, tableManager)

	_, err = executor.QueryExecutor("CREATE TABLE table_name (column1 VARCHAR(10),column2 int,column3 bool, column4 BIGINT, column5 float);")

	if err != nil {
		t.Fatal(err)
	}

	result, err := tableManager.GetTableMeta("table_name")

	if err != nil {
		t.Fatal(err)
	}

	expectResult := []*column.Column{
		{
			ColumnType: types.VAR_CHAR_TYPE,
			Name:       "column1",
			Size:       10,
		}, {
			ColumnType: types.INT_TYPE,
			Name:       "column2",
			Size:       4,
		}, {
			ColumnType: types.BOOL_TYPE,
			Name:       "column3",
			Size:       4,
		}, {
			ColumnType: types.LONG_INT_TYPE,
			Name:       "column4",
			Size:       8,
		}, {
			ColumnType: types.FLOAT_TYPE,
			Name:       "column5",
			Size:       8,
		},
	}

	if !reflect.DeepEqual(expectResult, result) {
		t.Error("create table wrong")
	}
}
