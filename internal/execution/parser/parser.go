package parser

import (
	"errors"
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
	case SELECT:
		_ast, err = ast.SelectAst(query, &scan)

	case INSERT:
		_ast, err = ast.InsertAst(query, &scan)
	case UPDATE:
	}

	if err != nil {
		return nil, err
	}

	return _ast, nil
}
