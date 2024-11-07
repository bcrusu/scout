package graph

import (
	"github.com/bcrusu/scout/internal/errors"
)

func (r *VertexId) Validate() error {
	if r == nil {
		return errors.Error("VertexId is nil")
	}
	if r.Type == 0 || len(r.Value) == 0 {
		return errors.Error("VertexId has missing fields")
	}
	return nil
}

func (r *Vertex) Validate() error {
	if r == nil {
		return errors.Error("Vertex is nil")
	}
	if err := r.Validate(); err != nil {
		return errors.Wrap(err, "Vertex.Id is invalid")
	}
	for id, value := range r.Properties {
		if id == 0 {
			return errors.Error("Vertex.Properties has invalid key")
		}
		if err := value.Validate(); err != nil {
			return errors.Wrapf(err, "Vertex.Properties[%d] is invalid", id)
		}
	}
	return nil
}

func (r *EdgeId) Validate() error {
	if r == nil {
		return errors.Error("EdgeId is nil")
	}
	if r.Type == 0 {
		return errors.Error("EdgeId invalid type")
	}
	if err := r.Head.Validate(); err != nil {
		return errors.Wrap(err, "EdgeId.Head is invalid")
	}
	if err := r.Tail.Validate(); err != nil {
		return errors.Wrap(err, "EdgeId.Tail is invalid")
	}
	return nil
}

func (r *Edge) Validate() error {
	if r == nil {
		return errors.Error("Edge is nil")
	}
	if err := r.Validate(); err != nil {
		return errors.Wrap(err, "Edge.Id is invalid")
	}
	for id, value := range r.Properties {
		if id == 0 {
			return errors.Error("Edge.Properties has invalid key")
		}
		if err := value.Validate(); err != nil {
			return errors.Wrapf(err, "Edge.Properties[%d] is invalid", id)
		}
	}
	return nil
}

func (r *GetVertexRequest) Validate() error {
	if r == nil {
		return errors.Error("GetVertexRequest is nil")
	}
	if err := r.Id.Validate(); err != nil {
		return errors.Wrap(err, "GetVertexRequest.Id is invalid")
	}
	return nil
}

func (r *GetEdgeRequest) Validate() error {
	if r == nil {
		return errors.Error("GetEdgeRequest is nil")
	}
	if err := r.Id.Validate(); err != nil {
		return errors.Wrap(err, "GetEdgeRequest.Id is invalid")
	}
	return nil
}

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
