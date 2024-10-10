package kv

import (
	"iter"
)

type DB interface {
	InitPartition(pid uint32) error
	DropPartition(pid uint32) error
	SyncPartition(pid uint32) error
	GetIndex(pid uint32, persistedOnDisk bool) (uint64, error)
	Put(pid uint32, index uint64, entries ...Entry) error
	Get(pid uint32, address Address) (*Entry, error)
	GetFrom(pid uint32, start Address) (Iterator, error)
	GetStream(pid uint32, start Address) (Iterator, error)
}

type Entry struct {
	Address Address
	Data    []byte
}

type Iterator = iter.Seq2[Entry, error]
