package txn

import (
	"fmt"

	"github.com/bcrusu/scout/internal/utils"
)

func mustEncodeValue(payload any) []byte {
	var value *Value

	switch x := payload.(type) {
	case []byte:
		value = &Value{Payload: &Value_Bytes{Bytes: x}}
	default:
		panic(fmt.Sprintf("unhandled payload type %T", payload))
	}

	bytes, err := utils.MarshalProto(value)
	if err != nil {
		panic(fmt.Sprintf("failed to encode value %s", value))
	}

	return bytes
}

func decodeValue(bytes []byte) (*Value, error) {
	return utils.UnmarshalProto[Value](bytes)
}
