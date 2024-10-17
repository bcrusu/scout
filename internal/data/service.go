package data

import (
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
)

func (a *KVAddress) Address() kv.Address {
	return kv.NewAddress(a.Keyspace, a.Key, a.Timestamp)
}

func (e *KVRecord) Record() kv.Record {
	return kv.Record{
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

func (a *KVRecord) Validate() error {
	if a == nil {
		return errors.Error("KVRecord is nil")
	}
	if err := a.Address.Validate(); err != nil {
		return errors.Wrap(err, "KVRecord.Address is invalid")
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
	for _, e := range r.Records {
		if err := e.Validate(); err != nil {
			return errors.Wrap(err, "StreamResponse.Records is invalid")
		}
	}
	return nil
}
