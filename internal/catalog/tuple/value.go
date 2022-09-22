package tuple

import (
	"go-db/internal/common/types"
	"reflect"
	"strconv"
)

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
		if reflect.TypeOf(value).Kind() == reflect.String {
			if value.(string) == "true" {
				value = true
			} else if value.(string) == "false" {
				value = false
			} else {
				return nil
			}
		}

		v.BOOL = value.(bool)
	case types.FLOAT_TYPE:
		if reflect.TypeOf(value).Kind() == reflect.String {
			floatValue, err := strconv.ParseFloat(value.(string), 64)
			if err != nil {
				return nil
			}
			value = floatValue
		}
		v.FLOAT = value.(float64)
	case types.INT_TYPE:
		if reflect.TypeOf(value).Kind() == reflect.String {
			int32Value, err := strconv.ParseInt(value.(string), 10, 32)
			if err != nil {
				return nil
			}
			value = int32(int32Value)
		}
		v.INT = value.(int32)
	case types.LONG_INT_TYPE:
		if reflect.TypeOf(value).Kind() == reflect.String {
			longIntValue, err := strconv.ParseInt(value.(string), 10, 64)
			if err != nil {
				return nil
			}
			value = longIntValue
		}
		v.LONG_INT = value.(int64)
	case types.VAR_CHAR_TYPE:
		if reflect.TypeOf(value).Kind() == reflect.String {
			value = []byte(value.(string))
		}
		v.VAR_CHAR = value.([]byte)
		if len(v.VAR_CHAR) > int(valueSize) {
			v.VAR_CHAR = v.VAR_CHAR[:valueSize]
		}
	}
	return &v
}

func GetValueInterface(value *Value) interface{} {
	switch value.GetType() {
	case types.BOOL_TYPE:
		return value.BOOL
	case types.FLOAT_TYPE:
		return value.FLOAT
	case types.INT_TYPE:
		return value.INT
	case types.LONG_INT_TYPE:
		return value.LONG_INT
	case types.VAR_CHAR_TYPE:
		return string(value.VAR_CHAR)
	}
	return nil
}

func GetDefaultValue(columType types.COLUMN_TYPE) interface{} {
	switch columType {
	case types.BOOL_TYPE:
		return false
	case types.FLOAT_TYPE:
		return 0.0
	case types.INT_TYPE:
		return int32(0)
	case types.LONG_INT_TYPE:
		return int64(0)
	case types.VAR_CHAR_TYPE:
		return []byte("")
	}
	return nil
}

func (v *Value) GetSize() int32 {
	return v.size
}

func (v *Value) GetType() types.COLUMN_TYPE {
	return v.types
}
