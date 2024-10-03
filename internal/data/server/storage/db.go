package storage

import (
	"encoding/base64"
	"fmt"
	"iter"
)

type DB interface {
	Get(Location) (*ValueAt, error)
	GetRange(Range) (Iterator, error)
	Exists(Location) (bool, error)
	ExistsInRange(Range) (bool, error)
	Set(Location, []byte) error
	Delete(Location) error
}

type Location struct {
	PartitionID uint32
	Keyspace    uint64
	Key         []byte
	Timestamp   uint64 // optional; if not specified, it represents the latest value
}

type Range struct {
	PartitionID uint32
	Keyspace    uint64
	StartKey    []byte // inclusive
	EndKey      []byte // exclusive
	Timestamp   uint64 // optional; if not specified, it represents the latest value
}

type ValueAt struct {
	Data     []byte
	Location Location
}

type Iterator = iter.Seq2[ValueAt, error]

func (l Location) String() string {
	return fmt.Sprintf("partition=%d keyspace=%d key=%s timestamp=%d",
		l.PartitionID, l.Keyspace, base64.RawURLEncoding.EncodeToString(l.Key), l.Timestamp)
}
