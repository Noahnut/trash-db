package schema

import (
	"go-db/internal/buffer"
	"go-db/internal/catalog/column"
	"go-db/internal/common/types"
	"go-db/internal/storage/disk"
	"log"
	"testing"
)

func Test_SetSchema(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	newPage, err := bufferPool.NewPage()

	if err != nil {
		t.Fatal(err)
	}

	schema := GetSchema(newPage)

	tableName := "tableTest"

	schema.SetTableName("tableTest")

	getTableName := schema.GetTableName()

	if tableName != getTableName {
		t.Error("get table name or set table name false", len(tableName), len(getTableName))
	}

	dataPage, err := bufferPool.NewPage()

	if err != nil {
		t.Fatal(err)
	}

	schema.SetDataPageID(dataPage.GetPageID())

	getDataPageID := schema.GetDataPageID()

	if getDataPageID != dataPage.GetPageID() {
		t.Error("set data page ID or get data page ID false")
	}

	schema.SetColumnCount(0)

	columnCount := schema.GetColumnCount()

	if columnCount != 0 {
		t.Error("init column count false")
	}

	columns := make([]*column.Column, 0)
	columns = append(columns, column.NewColumn(types.BOOL_TYPE, 0, "bool_type"))
	columns = append(columns, column.NewColumn(types.FLOAT_TYPE, 0, "float_type"))
	columns = append(columns, column.NewColumn(types.INT_TYPE, 0, "int_types"))
	columns = append(columns, column.NewColumn(types.LONG_INT_TYPE, 0, "long_int_type"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type"))

	for _, c := range columns {
		schema.AddColumn(c)
	}

	columnCount = schema.GetColumnCount()

	if columnCount != int32(len(columns)) {
		t.Error("add column wrong not match the expect columns len")
	}

	getTableName = schema.GetTableName()

	if tableName != getTableName {
		t.Error("get table name or set table name false", len(tableName), len(getTableName))
	}

	getDataPageID = schema.GetDataPageID()

	if getDataPageID != dataPage.GetPageID() {
		t.Error("set data page ID or get data page ID false")
	}

	getColumns := schema.GetColumns()

	for i, c := range getColumns {
		if c.ColumnType != columns[i].ColumnType {
			t.Error("column type not equal")
		}

		if c.Name != columns[i].Name {
			t.Error("column name not equal")
		}

		if c.Size != columns[i].Size {
			t.Error("column size not equal")
		}
	}

	columnIndexTwo, err := schema.GetColumnByIndex(2)

	if err != nil {
		t.Fatal(err)
	}

	if columnIndexTwo.ColumnType != columns[2].ColumnType {
		t.Error("column index two type not equal")
	}

	if columnIndexTwo.Name != columns[2].Name {
		t.Error("column index two name not equal")
	}

	if columnIndexTwo.Size != columns[2].Size {
		t.Error("column index two size not equal")
	}

	_, err = schema.GetColumnByIndex(6)

	if err == nil {
		t.Error("getColumnByIndex not catch overflow error")
	}

}

func Test_SchemaOverflow(t *testing.T) {
	diskManager, err := disk.NewDiskStorage("test.db")

	if err != nil {
		log.Fatal(err)
	}

	bufferPool := buffer.NewBufferPoolManager(buffer.NewLRUReplacer(), diskManager, 1024)

	newPage, err := bufferPool.NewPage()

	if err != nil {
		t.Fatal(err)
	}

	schema := GetSchema(newPage)

	tableName := "tableTest"

	schema.SetTableName("tableTest")

	getTableName := schema.GetTableName()

	if tableName != getTableName {
		t.Error("get table name or set table name false", len(tableName), len(getTableName))
	}

	dataPage, err := bufferPool.NewPage()

	if err != nil {
		t.Fatal(err)
	}

	schema.SetDataPageID(dataPage.GetPageID())

	getDataPageID := schema.GetDataPageID()

	if getDataPageID != dataPage.GetPageID() {
		t.Error("set data page ID or get data page ID false")
	}

	schema.SetColumnCount(0)

	columnCount := schema.GetColumnCount()

	if columnCount != 0 {
		t.Error("init column count false")
	}

	columns := make([]*column.Column, 0)
	columns = append(columns, column.NewColumn(types.BOOL_TYPE, 0, "bool_type"))
	columns = append(columns, column.NewColumn(types.FLOAT_TYPE, 0, "float_type"))
	columns = append(columns, column.NewColumn(types.INT_TYPE, 0, "int_types"))
	columns = append(columns, column.NewColumn(types.LONG_INT_TYPE, 0, "long_int_type"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_1"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_2"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_3"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_4"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_5"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_6"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_7"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_8"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_9"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_10"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_11"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_12"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_13"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_14"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_15"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_16"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_17"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_18"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_19"))
	columns = append(columns, column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_20"))

	for _, c := range columns {
		schema.AddColumn(c)
	}

	overflow := schema.AddColumn(column.NewColumn(types.VAR_CHAR_TYPE, 0, "var_char_type_21"))

	if !overflow {
		t.Error("detect overflow error")
	}

	columnCount = schema.GetColumnCount()

	if columnCount != int32(len(columns))+1 {
		t.Error("add column wrong not match the expect columns len")
	}
}
