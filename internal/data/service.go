package data

import "github.com/bcrusu/scout/internal/data/server/storage/kv"

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
