package tuple

import "go-db/internal/common/types"

type Value struct {
	types types.COLUMN_TYPE
	size  int32

	INT      int32
	LONG_INT int64
	FLOAT    float64
	VAR_CHAR []byte
	BOOL     bool
}

func GetValue(value interface{}, ValueType types.COLUMN_TYPE, valueSize int32) *Value {
	v := Value{
		types: ValueType,
		size:  valueSize,
	}
	switch ValueType {
	case types.BOOL_TYPE:
		v.BOOL = value.(bool)
	case types.FLOAT_TYPE:
		v.FLOAT = value.(float64)
	case types.INT_TYPE:
		v.INT = value.(int32)
	case types.LONG_INT_TYPE:
		v.LONG_INT = value.(int64)
	case types.VAR_CHAR_TYPE:
		v.VAR_CHAR = value.([]byte)
		if len(v.VAR_CHAR) > int(valueSize) {
			v.VAR_CHAR = v.VAR_CHAR[:valueSize]
		}
	}
	return &v
}

func (v *Value) GetSize() int32 {
	return v.size
}

func (v *Value) GetType() types.COLUMN_TYPE {
	return v.types
}
