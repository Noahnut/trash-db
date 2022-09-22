package ast

import (
	"go-db/internal/common/errors"
	"go-db/internal/common/types"
	"strconv"
	"strings"
	"text/scanner"
)

type Ast struct {
	Type   string
	Table  string
	Column []string
	Value  []interface{}
	Limit  int
}

/*
INSERT INTO table_name (column1, column2, column3...)
VALUES (value1, value2, value3...);

*/

func InsertAst(query string, scan *scanner.Scanner) (*Ast, error) {
	ast := &Ast{
		Type: types.INSERT_QUERY_TYPE,
	}

	var tokenString string

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		tokenString = scan.TokenText()

		if strings.ToUpper(tokenString) != types.QUERY_CHAR_INTO {
			return nil, errors.ErrSyntax
		}
	}

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		ast.Table = scan.TokenText()
	}

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		tokenString = scan.TokenText()

		if tokenString != "(" {
			return nil, errors.ErrSyntax
		}

		for {
			if token := scan.Scan(); token == scanner.EOF {
				return nil, errors.ErrSyntax
			} else {
				tokenString = scan.TokenText()
				if tokenString == ")" {
					break
				} else if tokenString == "," {
					continue
				} else {
					ast.Column = append(ast.Column, tokenString)
				}
			}
		}
	}

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		if strings.ToUpper(scan.TokenText()) != "VALUES" {
			return nil, errors.ErrSyntax
		}
	}

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		tokenString = scan.TokenText()

		if tokenString != "(" {
			return nil, errors.ErrSyntax
		}

		for {
			if token := scan.Scan(); token == scanner.EOF {
				return nil, errors.ErrSyntax
			} else {
				tokenString = scan.TokenText()
				if tokenString == ")" {
					break
				} else if tokenString == "," {
					continue
				} else {
					ast.Value = append(ast.Value, tokenString)
				}
			}
		}
	}
	return ast, nil
}

/*

SELECT * FROM `table`
SELECT * FROM `table` LIMIT `number`

*/

func SelectAst(query string, scan *scanner.Scanner) (*Ast, error) {
	ast := &Ast{
		Type: types.SELECT_QUERY_TYPE,
	}

	for {
		if token := scan.Scan(); token == scanner.EOF {
			return nil, errors.ErrSyntax
		} else {
			tokenString := scan.TokenText()

			if tokenString == types.QUERY_CHAR_STAR {
				ast.Column = append(ast.Column, types.QUERY_CHAR_STAR)
			} else {
				if tokenString == "," {
					continue
				} else if strings.ToUpper(tokenString) == types.QUERY_CHAR_FROM {
					break
				} else {
					ast.Column = append(ast.Column, tokenString)
				}
			}
		}
	}

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		ast.Table = scan.TokenText()
	}

	limitFlag := false
	for {
		if token := scan.Scan(); token == scanner.EOF {
			break
		} else {
			tokenText := scan.TokenText()

			if limitFlag {
				limitNumber, err := strconv.Atoi(tokenText)

				if err != nil {
					return nil, err
				}

				ast.Limit = limitNumber
			} else if strings.ToUpper(tokenText) == types.QUERY_CHAR_LIMIT {
				limitFlag = true
			}
		}
	}

	return ast, nil
}
