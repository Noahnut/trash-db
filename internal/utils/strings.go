package utils

import "bytes"

func ConvertByteToString(data []byte) string {
	return string(bytes.ReplaceAll(data, []byte{0}, []byte{}))
}
