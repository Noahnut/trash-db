package ast

import (
	"fmt"
	"go-db/internal/common/errors"
	"go-db/internal/common/types"
	"strconv"
	"strings"
	"text/scanner"
)

type Ast struct {
	Type       string
	Table      string
	Column     []string
	ColumnType []string
	Value      []interface{}
	Limit      int
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

		if tokenString != types.QUERY_CHAR_LEFT_PARE_BRACKETS {
			return nil, errors.ErrSyntax
		}

		for {
			if token := scan.Scan(); token == scanner.EOF {
				return nil, errors.ErrSyntax
			} else {
				tokenString = scan.TokenText()
				if tokenString == types.QUERY_CHAR_RIGHT_PARE_BRACKETS {
					break
				} else if tokenString == types.QUERY_CHAR_COMMA {
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
		if strings.ToUpper(scan.TokenText()) != types.QUERY_CHAR_VALUE {
			return nil, errors.ErrSyntax
		}
	}

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		tokenString = scan.TokenText()

		if tokenString != types.QUERY_CHAR_LEFT_PARE_BRACKETS {
			return nil, errors.ErrSyntax
		}

		for {
			if token := scan.Scan(); token == scanner.EOF {
				return nil, errors.ErrSyntax
			} else {
				tokenString = scan.TokenText()
				if tokenString == types.QUERY_CHAR_RIGHT_PARE_BRACKETS {
					break
				} else if tokenString == types.QUERY_CHAR_COMMA {
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
				if tokenString == types.QUERY_CHAR_COMMA {
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

/*

CREATE TABLE table_name (
    column1 datatype,
    column2 datatype,
    column3 datatype,
);

*/
func CreateTableAst(query string, scan *scanner.Scanner) (*Ast, error) {
	ast := &Ast{
		Type: types.CREATE_QUERY_TYPE,
	}

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		if strings.ToUpper(scan.TokenText()) != types.QUERY_CHAR_TABLE {
			return nil, errors.ErrSyntax
		}

		if token := scan.Scan(); token == scanner.EOF {
			return nil, errors.ErrSyntax
		} else {
			ast.Table = scan.TokenText()
		}
	}

	var (
		columnName string
		columnType string
	)

	if token := scan.Scan(); token == scanner.EOF {
		return nil, errors.ErrSyntax
	} else {
		if scan.TokenText() != types.QUERY_CHAR_LEFT_PARE_BRACKETS {
			return nil, errors.ErrSyntax
		}
	}

	for {
		if token := scan.Scan(); token == scanner.EOF {
			return nil, errors.ErrSyntax
		} else {
			columnName = scan.TokenText()

			if columnName == types.QUERY_CHAR_COMMA {
				continue
			}

			if columnName == types.QUERY_CHAR_RIGHT_PARE_BRACKETS {
				break
			}

			if token := scan.Scan(); token == scanner.EOF {
				return nil, errors.ErrSyntax
			} else {

				columnType = checkColumnTypeIsValid(scan.TokenText())

				if columnType == types.COLUMN_TYPE_INVALID {
					return nil, errors.ErrSyntax
				}

				if columnType == types.COLUMN_TYPE_VAR_CHAR {
					for {
						if token := scan.Scan(); token == scanner.EOF {
							break
						}

						tokenString := scan.TokenText()

						if tokenString == types.QUERY_CHAR_LEFT_PARE_BRACKETS {
							continue
						} else if tokenString == types.QUERY_CHAR_RIGHT_PARE_BRACKETS {
							break
						} else {
							varcharSize, err := strconv.Atoi(tokenString)
							if err != nil {
								return nil, errors.ErrSyntax
							}

							columnType = fmt.Sprintf("%s(%d)", types.COLUMN_TYPE_VAR_CHAR, varcharSize)
						}
					}
				}

				ast.Column = append(ast.Column, columnName)
				ast.ColumnType = append(ast.ColumnType, columnType)
			}
		}
	}

	return ast, nil
}

func checkColumnTypeIsValid(columnType string) string {

	upperCaseColumn := strings.ToUpper(columnType)

	switch upperCaseColumn {
	case types.COLUMN_TYPE_BOOL:
		return types.COLUMN_TYPE_BOOL
	case types.COLUMN_TYPE_FLOAT:
		return types.COLUMN_TYPE_FLOAT
	case types.COLUMN_TYPE_INT:
		return types.COLUMN_TYPE_INT
	case types.COLUMN_TYPE_LONGINT:
		return types.COLUMN_TYPE_LONGINT
	}

	if strings.HasPrefix(upperCaseColumn, types.COLUMN_TYPE_VAR_CHAR) {
		return upperCaseColumn
	}

	return types.COLUMN_TYPE_INVALID
}
