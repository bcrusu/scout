package graph

import (
	"github.com/bcrusu/scout/internal/errors"
)

func (r *Status) Validate() error {
	if r == nil {
		return errors.Error("Status is nil")
	}
	if r.Timestamp == nil {
		return errors.Error("Status has missing fields")
	}
	return nil
}

func (r *Value) Validate() error {
	if r == nil {
		return errors.Error("Value is nil")
	}

	switch x := r.Payload.(type) {
	case nil:
		return errors.Error("Value.Payload is nil")
	case *Value_Bool, *Value_Str, *Value_Int32, *Value_Uint32, *Value_Int64, *Value_Uint64, *Value_Float32, *Value_Float64, *Value_Bytes:
		// pass
	case *Value_List:
		if x.List == nil || x.List.Items == nil {
			return errors.Error("Value.List is nil")
		}
	case *Value_Map:
		if x.Map == nil || x.Map.Items == nil {
			return errors.Error("Value.Map is nil")
		}
	case *Value_Timestamp:
		if x.Timestamp == nil {
			return errors.Error("ValueTimestamp is nil")
		}
	default:
		return errors.Error("Value.Payload is unknown.")
	}
	return nil
}
