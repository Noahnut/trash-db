package table

import (
	"go-db/internal/buffer"
	"go-db/internal/catalog/column"
	"go-db/internal/catalog/schema"
	"go-db/internal/catalog/tuple"
	"go-db/internal/common/types"
	"go-db/internal/storage/disk"
	"go-db/internal/utils"
	"log"
	"strings"
	"testing"
)

func Test_InsertTuple(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	newPage, err := bufferPool.NewPage()

	if err != nil {
		t.Fatal(err)
	}

	schema := schema.GetSchema(newPage)

	tableName := "tableTest"

	schema.SetTableName("tableTest")

	getTableName := schema.GetTableName()

	if tableName != getTableName {
		t.Error("get table name or set table name false", len(tableName), len(getTableName))
	}

	newPage, err = bufferPool.NewPage()

	if err != nil {
		t.Fatal(err)
	}

	dataPage := GetDataTable(newPage)
	dataPage.DataTableInit()

	columns := make([]*column.Column, 0)

	columns = append(columns, column.NewColumn(types.BOOL_TYPE, 0, "bool_type"))
	columns = append(columns, column.NewColumn(types.FLOAT_TYPE, 0, "float_type"))
	columns = append(columns, column.NewColumn(types.INT_TYPE, 0, "int_types"))
	columns = append(columns, column.NewColumn(types.LONG_INT_TYPE, 0, "long_int_type"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type"))

	for _, c := range columns {
		schema.AddColumn(c)
	}

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

	err = dataPage.InsertTuple(tuples, int32(valueSize))

	if err != nil {
		t.Fatal(err)
	}

	getTuples := dataPage.GetTuple(schema)

	if len(getTuples) != 1 {
		t.Error("get tuple wrong")
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
