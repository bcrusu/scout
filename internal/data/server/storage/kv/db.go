package kv

import (
	"iter"
)

type DB interface {
	InitPartition(pid uint32) error
	DropPartition(pid uint32) error
	SyncPartition(pid uint32) error
	GetIndex(pid uint32, persistedOnDisk bool) (uint64, error)
	Put(pid uint32, index uint64, records ...Record) error
	Get(pid uint32, address Address) (*Record, error)
	GetRange(pid uint32, start Address, end *Address) (Iterator, error)
	GetStream(pid uint32, start Address) (Iterator, error)
}

type Record struct {
	Address Address
	Data    []byte
}

type Iterator = iter.Seq2[Record, error]
