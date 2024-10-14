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

func (r *AutocommitRequest) IsSnapshotRead() bool {
	return r.ReadTimestamp != 0 && r.Txn.IsReadOnly()
}

func (r *AutocommitRequest) Validate() error {
	if r == nil {
		return errors.Error("AutocommitRequest is nil")
	}
	if err := r.Txn.Validate(); err != nil {
		return errors.Wrap(err, "AutocommitRequest.Txn is invalid")
	}
	if r.ReadTimestamp != 0 && !r.Txn.IsReadOnly() {
		return errors.Error("AutocommitRequest.ReadTimestamp invalid for read-write txn.")
	}
	return nil
}

func (r *PrepareRequest) Validate() error {
	if r == nil {
		return errors.Error("PrepareRequest is nil")
	}
	if err := r.Txn.Validate(); err != nil {
		return errors.Wrap(err, "PrepareRequest.Txn is invalid")
	}
	return nil
}

func (r *CommitRequest) Validate() error {
	if r == nil {
		return errors.Error("CommitRequest is nil")
	}
	if err := r.Id.Validate(); err != nil {
		return errors.Wrap(err, "PrepareRequest.Id is invalid")
	}
	if r.CommitTimestamp == 0 {
		return errors.Error("CommitRequest has missing fields")
	}
	return nil
}

func (r *AbortRequest) Validate() error {
	if r == nil {
		return errors.Error("AbortRequest is nil")
	}
	if err := r.Id.Validate(); err != nil {
		return errors.Wrap(err, "AbortRequest.Id is invalid")
	}
	return nil
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
