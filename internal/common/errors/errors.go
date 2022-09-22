package errors

import "errors"

var (
	ErrPageNotFound          = errors.New("page not found")
	ErrColumnIndexOutOfRange = errors.New("column index out of range")
)

var (
	ErrNoPageCanReplace = errors.New("no page can replace")
)

var (
	ErrNoSpace = errors.New("no enough space for insert tuple")
)

var (
	ErrNoTable = errors.New("table not exist")
)

var (
	ErrIndexOutOfRange = errors.New("index out of range")
)

var (
	ErrSyntax         = errors.New("syntax error")
	ErrColumnNotExist = errors.New("column not exist")
)
