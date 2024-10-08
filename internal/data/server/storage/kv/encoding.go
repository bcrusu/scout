package kv

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

const (
	FlagEmpty     Flags = 0
	FlagTombstone Flags = 1
)

type Flags byte

func (f Flags) Tombstone() bool {
	return (f & FlagTombstone) != 0
}

// +----------+---------+-----------+--------+
// | Keyspace |   Key   | Timestamp | Flags  |
// |  4 Bytes | N Bytes |  8 Bytes  | 1 Byte |
// +----------+---------+-----------+--------+
func EncodeKey(addr Address, flags Flags) []byte {
	result := make([]byte, 0, 13+len(addr.Key))

	result = binary.LittleEndian.AppendUint32(result, addr.Keyspace)
	result = append(result, addr.Key...)
	result = binary.LittleEndian.AppendUint64(result, addr.Timestamp)
	result = append(result, byte(flags))
	return result
}

func DecodeKey(key []byte) (Address, Flags) {
	if len(key) < 13 {
		panic(fmt.Sprintf("cannot decode invalid key %s", base64.RawURLEncoding.EncodeToString(key)))
	}

	addr := Address{
		Keyspace:  ExtractKeyspace(key),
		Key:       ExtractKey(key),
		Timestamp: ExtractTimestamp(key),
	}

	return addr, ExtractFlags(key)
}

func ExtractKeyspace(key []byte) uint32 {
	return binary.LittleEndian.Uint32(key[0:4])
}

func ExtractKey(key []byte) []byte {
	return key[4 : len(key)-9]
}

func ExtractTimestamp(key []byte) uint64 {
	l := len(key)
	return binary.LittleEndian.Uint64(key[l-9 : l-1])
}

func ExtractFlags(key []byte) Flags {
	return Flags(key[len(key)-1])
}
