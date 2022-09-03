package column

import "go-db/internal/common/types"

type Column struct {
	ColumnType types.COLUMN_TYPE
	Name       string
	Size       int32
}

func NewColumn(types types.COLUMN_TYPE, size int32, name string) *Column {
	c := &Column{
		ColumnType: types,
		Name:       name,
	}

	c.SetTypeSize(size)

	return c
}

func (c *Column) SetTypeSize(size int32) {
	switch c.ColumnType {
	case types.BOOL_TYPE:
		size = types.BOOL_SIZE
	case types.FLOAT_TYPE:
		size = types.FLOAT_SIZE
	case types.INT_TYPE:
		size = types.INT_SIZE
	case types.LONG_INT_TYPE:
		size = types.LONG_INT_SIZE
	case types.VAR_CHAR_TYPE:
		if size == 0 {
			size = types.VAR_CHAR_SIZE
		}
	}
	c.Size = size
}

func (c *Column) GetColumnName() string {
	return c.Name
}

func (c *Column) GetColumnType() types.COLUMN_TYPE {
	return c.ColumnType
}

func (c *Column) GetColumnSize() int32 {
	return c.Size
}
