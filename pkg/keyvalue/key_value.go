package keyvalue

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
)

func (r *GetRequest) Validate() error {
	if r == nil {
		return errors.Error("GetRequest is nil")
	}
	if r.Snapshot != nil && r.Snapshot.AsTime().After(time.Now()) {
		return errors.Error("GetRequest has invalid snapshot timestamp")
	}
	for _, key := range r.Keys {
		if len(key) == 0 {
			return errors.Error("GetRequest has empty key")
		}
	}
	return nil
}

func (r *GetResponse) Validate() error {
	if r == nil {
		return errors.Error("GetResponse is nil")
	}
	if r.Timestamp == nil {
		return errors.Error("GetResponse has invalid timestamp")
	}
	return nil
}

func (r *SetRequest) Validate() error {
	if r == nil {
		return errors.Error("SetRequest is nil")
	}
	if len(r.Items) == 0 {
		return errors.Error("SetRequest is empty")
	}
	for _, kv := range r.Items {
		if err := kv.Validate(); err != nil {
			return errors.Wrap(err, "SetRequest has invalid items")
		}
	}
	return nil
}

func (r *DeleteRequest) Validate() error {
	if r == nil {
		return errors.Error("DelRequest is nil")
	}
	for _, key := range r.Keys {
		if len(key) == 0 {
			return errors.Error("DelRequest has empty key")
		}
	}
	return nil
}

func (r *KeyValue) Validate() error {
	if r == nil {
		return errors.Error("KeyValue is nil")
	}
	if len(r.Key) == 0 {
		return errors.Error("KeyValue.Key is empty")
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
