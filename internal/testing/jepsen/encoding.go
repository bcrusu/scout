package jepsen

import "encoding/binary"

func encodeKey(key string) []byte {
	return []byte(key)
}

func decodeKey(data []byte) string {
	return string(data)
}

func encodeValue(value uint32) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func decodeValue(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}
