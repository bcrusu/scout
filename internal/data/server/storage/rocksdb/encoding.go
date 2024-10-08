package rocksdb

import (
	"encoding/base64"
	"encoding/binary"

	"github.com/bcrusu/graph/internal/errors"
)

func encodeUint32(v uint32) []byte {
	result := make([]byte, 4)
	binary.LittleEndian.PutUint32(result, v)
	return result
}

func encodeUint64(v uint64) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, v)
	return result
}

func decodeUint32(data []byte) (uint32, error) {
	if len(data) != 4 {
		return 0, errors.Errorf("invalid uint32 data=%s", base64.RawURLEncoding.EncodeToString(data))
	}

	return binary.LittleEndian.Uint32(data), nil
}

func decodeUint64(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.Errorf("invalid uint64 data=%s", base64.RawURLEncoding.EncodeToString(data))
	}

	return binary.LittleEndian.Uint64(data), nil
}
