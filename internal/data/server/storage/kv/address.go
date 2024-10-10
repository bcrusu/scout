package kv

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

type Address struct {
	Keyspace  uint32
	Key       []byte
	Timestamp uint64
}

func NewAddress(kyespace uint32, key []byte, timestamp uint64) Address {
	return Address{
		Keyspace:  kyespace,
		Key:       key,
		Timestamp: timestamp,
	}
}

func FirstAddress(keyspace uint32) Address {
	return Address{
		Keyspace:  keyspace,
		Key:       []byte{},
		Timestamp: math.MaxUint64,
	}
}

// +----------+---------+-----------+
// | Keyspace |   Key   | Timestamp |
// |  4 Bytes | N Bytes |  8 Bytes  |
// +----------+---------+-----------+
func (a Address) Encode() []byte {
	result := make([]byte, 0, 12+len(a.Key))

	result = binary.LittleEndian.AppendUint32(result, a.Keyspace)
	result = append(result, a.Key...)
	result = binary.LittleEndian.AppendUint64(result, a.Timestamp)
	return result
}

func (a Address) Next() Address {
	timestamp := a.Timestamp - 1
	if timestamp != math.MaxUint64 {
		return NewAddress(a.Keyspace, a.Key, timestamp)
	}

	key := slices.Clone(a.Key)
	for i := range key {
		key[i] = key[i] + 1
		if key[i] != 0 {
			return NewAddress(a.Keyspace, key, timestamp)
		}
	}

	return NewAddress(a.Keyspace+1, key, timestamp)
}

func (a Address) String() string {
	return fmt.Sprintf("keyspace=%d key=%s timestamp=%d",
		a.Keyspace, base64.RawURLEncoding.EncodeToString(a.Key), a.Timestamp)
}

func DecodeAddress(key []byte) Address {
	l := len(key)
	if l < 12 {
		panic(fmt.Sprintf("cannot decode invalid key %s", base64.RawURLEncoding.EncodeToString(key)))
	}

	return Address{
		Keyspace:  binary.LittleEndian.Uint32(key[0:4]),
		Key:       key[4 : len(key)-8],
		Timestamp: binary.LittleEndian.Uint64(key[l-8:]),
	}
}
