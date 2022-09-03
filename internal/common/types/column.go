package types

type COLUMN_TYPE int32

const (
	_ COLUMN_TYPE = iota
	VAR_CHAR_TYPE
	INT_TYPE
	LONG_INT_TYPE
	FLOAT_TYPE
	BOOL_TYPE
)

const (
	COLUMN_NAME_OFFSET = 128
	COLUMN_TYPE_OFFSET = 4
	COLUMN_SIZE_OFFSET = 4
)

const (
	BOOL_SIZE     = 4
	INT_SIZE      = 4
	FLOAT_SIZE    = 8
	LONG_INT_SIZE = 8
	VAR_CHAR_SIZE = 52
)