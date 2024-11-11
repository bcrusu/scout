package rocksdb

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/bcrusu/scout/internal/errors"
)

var (
	minUint64 = encodeUint64(0)
	maxUint64 = encodeUint64(math.MaxUint64)
)

func encodeUint32(v uint32) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, v)
	return result
}

func encodeUint64(v uint64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, v)
	return result
}

func decodeUint32(data []byte) (uint32, error) {
	if len(data) != 4 {
		return 0, errors.Errorf("invalid uint32 data=%s", base64.RawURLEncoding.EncodeToString(data))
	}

	return binary.BigEndian.Uint32(data), nil
}

func decodeUint64(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.Errorf("invalid uint64 data=%s", base64.RawURLEncoding.EncodeToString(data))
	}

	return binary.BigEndian.Uint64(data), nil
}

// +----------+---------+
// | Keyspace |   Key   |
// |  4 Bytes | N Bytes |
// +----------+---------+
func encodeKey(keyspace uint32, key []byte) []byte {
	result := make([]byte, 0, 4+len(key))

	result = binary.BigEndian.AppendUint32(result, keyspace)
	result = append(result, key...)
	return result
}

func decodeKey(key []byte) (uint32, []byte) {
	l := len(key)
	if l < 4 {
		panic(fmt.Sprintf("invalid key length=%d. Key=%s", len(key), base64.RawURLEncoding.EncodeToString(key)))
	}

	return binary.BigEndian.Uint32(key[0:4]), slices.Clone(key[4:])
}
