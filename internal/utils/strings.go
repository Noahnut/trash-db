package utils

import "bytes"

func ConvertByteToString(data []byte) string {
	return string(bytes.ReplaceAll(data, []byte{0}, []byte{}))
}

func TrimByteEmptySpace(data []byte) []byte {
	return bytes.ReplaceAll(data, []byte{0}, []byte{})
}
