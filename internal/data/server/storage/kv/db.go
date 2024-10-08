package kv

import (
	"encoding/base64"
	"fmt"
	"iter"
)

type DB interface {
	InitPartition(pid uint32) error
	DropPartition(pid uint32) error
	SyncPartition(pid uint32) (uint64, error)
	Put(index uint64, pid uint32, entries ...Entry) error
	Get(pid uint32, start Address) (Iterator, error)
}

type Address struct {
	Keyspace  uint32
	Key       []byte
	Timestamp uint64
}

type Entry struct {
	Address Address
	Value   []byte
	Flags   Flags
}

type Iterator = iter.Seq2[Entry, error]

func NewAddress(kyespace uint32, key []byte, timestamp uint64) Address {
	return Address{
		Keyspace:  kyespace,
		Key:       key,
		Timestamp: timestamp,
	}
}

func (a Address) String() string {
	return fmt.Sprintf("keyspace=%d key=%s timestamp=%d",
		a.Keyspace, base64.RawURLEncoding.EncodeToString(a.Key), a.Timestamp)
}
