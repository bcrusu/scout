package mvcc

import (
	"encoding/base64"
	"fmt"
	"iter"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
)

type DB interface {
	Get(pid uint32, timestamp uint64, addrs ...Address) ([]*Record, error)
	GetRange(pid uint32, timestamp uint64, start, end Address) (Iterator, error)
	Exists(pid uint32, timestamp uint64, addr Address) (bool, error)
	ExistsInRange(pid uint32, timestamp uint64, start, end Address) (bool, error)
	Put(pid uint32, index uint64, records ...Record) error
}

type Address struct {
	Keyspace uint32
	Key      []byte
}

type Record struct {
	Address   Address
	Timestamp uint64
	Value     []byte
	Flags     Flags
}

type Iterator = iter.Seq2[Record, error]

func NewAddress(kyespace uint32, key []byte) Address {
	return Address{
		Keyspace: kyespace,
		Key:      key,
	}
}

func (r Record) ToKVRecord() kv.Record {
	return kv.Record{
		Address: kv.NewAddress(r.Address.Keyspace, r.Address.Key, r.Timestamp),
		Data:    EncodeData(r.Value, r.Flags),
	}
}

func (a Address) String() string {
	return fmt.Sprintf("keyspace=%d key=%s",
		a.Keyspace, base64.RawURLEncoding.EncodeToString(a.Key))
}
