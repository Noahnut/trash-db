package tuple

import (
	"encoding/binary"
	"go-db/internal/catalog/schema"
	"go-db/internal/common/types"
	"math"
)

func TupleSerialization(values []*Value) []byte {
	data := make([]byte, 0)
	for _, v := range values {
		tempData := make([]byte, v.size)

		switch v.types {
		case types.BOOL_TYPE:
			var boolValue uint32
			if v.BOOL {
				boolValue = 1
			}
			binary.BigEndian.PutUint32(tempData, boolValue)
		case types.INT_TYPE:
			binary.BigEndian.PutUint32(tempData, uint32(v.INT))
		case types.LONG_INT_TYPE:
			binary.BigEndian.PutUint64(tempData, uint64(v.LONG_INT))
		case types.VAR_CHAR_TYPE:
			copy(tempData, v.VAR_CHAR)
		case types.FLOAT_TYPE:
			binary.BigEndian.PutUint64(tempData, math.Float64bits(v.FLOAT))
		}
		data = append(data, tempData...)
	}

	return data
}

func TupleDeserialization(schema *schema.Schema, data []byte) []*Value {
	columns := schema.GetColumns()

	values := make([]*Value, 0, len(columns))
	byteOffset := 0
	for _, c := range columns {
		v := &Value{
			types: c.ColumnType,
			size:  c.Size,
		}

		switch v.types {
		case types.BOOL_TYPE:
			boolValue := binary.BigEndian.Uint32(data[byteOffset : byteOffset+int(v.size)])
			if boolValue == 1 {
				v.BOOL = true
			}
		case types.INT_TYPE:
			v.INT = int32(binary.BigEndian.Uint32(data[byteOffset : byteOffset+int(v.size)]))
		case types.LONG_INT_TYPE:
			v.LONG_INT = int64(binary.BigEndian.Uint64(data[byteOffset : byteOffset+int(v.size)]))
		case types.VAR_CHAR_TYPE:
			v.VAR_CHAR = make([]byte, v.size)
			copy(v.VAR_CHAR, data[byteOffset:byteOffset+int(v.size)])
		case types.FLOAT_TYPE:
			v.FLOAT = math.Float64frombits(binary.BigEndian.Uint64(data[byteOffset : byteOffset+int(v.size)]))
		}

		byteOffset += int(c.Size)
		values = append(values, v)
	}

	return values
}
