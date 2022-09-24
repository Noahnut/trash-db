package ast

import (
	"go-db/internal/common/types"
	"reflect"
	"strings"
	"testing"
	"text/scanner"
)

func Test_SelectAst(t *testing.T) {
	query := "Select * from tableTest"

	s := scanner.Scanner{}
	s.Init(strings.NewReader(query))

	if token := s.Scan(); token == scanner.EOF {
		t.Error("scan wrong")
	}

	ast, err := SelectAst(query, &s)

	if err != nil {
		t.Fatal(err)
	}

	if ast.Table != "tableTest" {
		t.Error("parse query wrong")
	}

	if len(ast.Column) != 1 {
		t.Error("parse select value wrong")
	}

	if ast.Column[0] != "*" {
		t.Error("parse value should be star")
	}

	query = "Select * from tableTest limit 10"

	s = scanner.Scanner{}
	s.Init(strings.NewReader(query))

	if token := s.Scan(); token == scanner.EOF {
		t.Error("scan wrong")
	}

	ast, err = SelectAst(query, &s)

	if err != nil {
		t.Fatal(err)
	}

	if ast.Table != "tableTest" {
		t.Error("parse query wrong")
	}

	if len(ast.Column) != 1 {
		t.Error("parse select value wrong")
	}

	if ast.Column[0] != "*" {
		t.Error("parse value should be star")
	}

	if ast.Limit != 10 {
		t.Error("parse limit should be 10")
	}

	query = "Select value1, value2, value3 from tableTest limit 10"

	s = scanner.Scanner{}
	s.Init(strings.NewReader(query))

	if token := s.Scan(); token == scanner.EOF {
		t.Error("scan wrong")
	}

	ast, err = SelectAst(query, &s)

	if err != nil {
		t.Fatal(err)
	}

	if ast.Table != "tableTest" {
		t.Error("parse query wrong")
	}

	if len(ast.Column) != 3 {
		t.Error("parse value wrong")
	}

	if ast.Column[0] != "value1" || ast.Column[1] != "value2" || ast.Column[2] != "value3" {
		t.Error("select value wrong")
	}

	if ast.Limit != 10 {
		t.Error("parse limit should be 10")
	}
}

func Test_InsertAst(t *testing.T) {
	query := "INSERT INTO table_name (column1, column2, column3) VALUES (value1, value2, value3)"

	s := scanner.Scanner{}
	s.Init(strings.NewReader(query))

	if token := s.Scan(); token == scanner.EOF {
		t.Error("scan wrong")
	}

	ast, err := InsertAst(query, &s)

	if err != nil {
		t.Fatal(err)
	}

	if ast.Type != types.INSERT_QUERY_TYPE {
		t.Error("get wrong insert type")
	}

	if ast.Table != "table_name" {
		t.Error("get the wrong table name")
	}

	if !reflect.DeepEqual(ast.Column, []string{"column1", "column2", "column3"}) {
		t.Error("get column name wrong")
	}

}

func Test_CreateAst(t *testing.T) {
	query := "CREATE TABLE table_name (column1 VARCHAR(10),column2 int,column3 bool, column4 BIGINT, column5 float);"

	s := scanner.Scanner{}
	s.Init(strings.NewReader(query))

	if token := s.Scan(); token == scanner.EOF {
		t.Error("scan wrong")
	}

	ast, err := CreateTableAst(query, &s)

	if err != nil {
		t.Fatal(err)
	}

	if ast.Type != types.CREATE_QUERY_TYPE {
		t.Error("create get the wrong type")
	}

	if ast.Table != "table_name" {
		t.Error("create get the wrong table name")
	}

	if !reflect.DeepEqual(ast.Column, []string{"column1", "column2", "column3", "column4", "column5"}) {
		t.Error("get the wrong column name")
	}

	if !reflect.DeepEqual(ast.ColumnType, []string{"VARCHAR(10)", "INT", "BOOL", "BIGINT", "FLOAT"}) {
		t.Error("get the wrong column type", ast.ColumnType)
	}
}
