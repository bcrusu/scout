package storage

import (
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/utils"
)

func mustEncodeValueBytes(loc Location, bytes []byte) []byte {
	v := &data.Value{Payload: &data.Value_Bytes{Bytes: bytes}}
	return mustEncodeValue(loc, v)
}

func mustEncodeValue(loc Location, v *data.Value) []byte {
	data, err := utils.MarshalProto(v)
	if err != nil {
		logT.WithError(err).Error("Failed to encode.", "value", v, "location", loc)
		panic("encode value failed")
	}
	return data
}

func decodeValue(bytes []byte) (*data.Value, error) {
	return utils.UnmarshalProto[data.Value](bytes)
}
