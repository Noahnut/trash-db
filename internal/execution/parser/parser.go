package parser

import (
	"errors"
	"go-db/internal/common/types"
	"go-db/internal/execution/ast"
	"strings"
	"text/scanner"
)

var (
	SELECT = "SELECT"
	INSERT = "INSERT"
	UPDATE = "UPDATE"
)

func ParseSQLQuery(query string) (*ast.Ast, error) {
	scan := scanner.Scanner{}
	scan.Init(strings.NewReader(query))

	var (
		_ast *ast.Ast
		err  error
	)

	if scan.Scan() == scanner.EOF {
		return nil, errors.New("")
	}

	queryType := scan.TokenText()

	switch strings.ToUpper(queryType) {
	case types.SELECT_QUERY_TYPE:
		_ast, err = ast.SelectAst(query, &scan)
	case types.INSERT_QUERY_TYPE:
		_ast, err = ast.InsertAst(query, &scan)
	case types.CREATE_QUERY_TYPE:
		_ast, err = ast.CreateTableAst(query, &scan)
	}

	if err != nil {
		return nil, err
	}

	return _ast, nil
}
