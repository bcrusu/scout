package rocksdb

import (
	"bytes"
	"encoding/base64"

	"github.com/linxGnu/grocksdb"
)

var (
	_ grocksdb.MergeOperator = (*mergeOperator)(nil)
	_ grocksdb.PartialMerger = (*mergeOperator)(nil)
	_ grocksdb.MultiMerger   = (*mergeOperator)(nil)
)

type mergeOperator struct{}

func (m *mergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	values := operands
	if len(existingValue) > 0 {
		values = append(operands, existingValue)
	}

	return m.mergeMaxIndex(key, values...)
}

func (m *mergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return m.mergeMaxIndex(key, leftOperand, rightOperand)
}

func (m *mergeOperator) PartialMergeMulti(key []byte, operands [][]byte) ([]byte, bool) {
	return m.mergeMaxIndex(key, operands...)
}

func (m *mergeOperator) Destroy() {}

func (m *mergeOperator) Name() string {
	return mergeOperatorName
}

func (m *mergeOperator) mergeMaxIndex(key []byte, operands ...[]byte) ([]byte, bool) {
	if !bytes.Equal(key, keyIndex) {
		log.Warn("Unexpected merge operator call.", "key", base64.RawURLEncoding.EncodeToString(key))
		return nil, false
	}

	result := uint64(0)

	for _, op := range operands {
		if x, err := decodeUint64(op); err != nil {
			log.WithError(err).Error("Failed to decode merge operands value.", "value", base64.RawURLEncoding.EncodeToString(op))
			return nil, false
		} else {
			result = max(result, x)
		}
	}

	return encodeUint64(result), true
}
