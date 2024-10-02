package storage

import (
	"fmt"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/utils"
)

func mustEncodeValue(payload any) []byte {
	var value *data.Value

	switch x := payload.(type) {
	case []byte:
		value = &data.Value{Payload: &data.Value_Bytes{Bytes: x}}
	default:
		panic(fmt.Sprintf("unhandled payload type %T", payload))
	}

	bytes, err := utils.MarshalProto(value)
	if err != nil {
		panic(fmt.Sprintf("failed to encode value %s", value))
	}

	return bytes
}

func decodeValue(bytes []byte) (*data.Value, error) {
	return utils.UnmarshalProto[data.Value](bytes)
}
