package data

import (
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
)

func (a *KVAddress) Address() kv.Address {
	return kv.Address{
		Keyspace:  a.Keyspace,
		Key:       a.Key,
		Timestamp: a.Timestamp,
	}
}

func (e *KVEntry) Entry() kv.Entry {
	return kv.Entry{
		Address: e.Address.Address(),
		Data:    e.Data,
	}
}

func (a *KVAddress) Validate() error {
	if a == nil {
		return errors.Error("KVAddress is nil")
	}
	if a.Keyspace < keyspace.FirstUserKeyspace || len(a.Key) == 0 {
		return errors.Error("KVAddress has invalid fields")
	}
	return nil
}

func (a *KVEntry) Validate() error {
	if a == nil {
		return errors.Error("KVAddress is nil")
	}
	if err := a.Address.Validate(); err != nil {
		return errors.Wrap(err, "KVEntry.Address is invalid")
	}
	return nil
}

func (r *StreamRequest) Validate() error {
	if r == nil {
		return errors.Error("StreamRequest is nil")
	}
	if r.StartAddress != nil {
		if err := r.StartAddress.Validate(); err != nil {
			return errors.Wrap(err, "StreamRequest.StartAddress is invalid")
		}
	}
	return nil
}

func (r *StreamResponse) Validate() error {
	if r == nil {
		return errors.Error("StreamResponse is nil")
	}
	for _, e := range r.Entries {
		if err := e.Validate(); err != nil {
			return errors.Wrap(err, "StreamResponse.Entries is invalid")
		}
	}
	return nil
}
