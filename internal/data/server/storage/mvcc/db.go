package mvcc

import (
	"bytes"
	"fmt"
	"iter"
	"math"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
)

type DB interface {
	Get(kv.Address) (*Entry, error)
	GetRange(Range) (Iterator, error)
	Exists(kv.Address) (bool, error)
	ExistsInRange(Range) (bool, error)
	Put(index uint64, entries ...Entry) error
}

type Entry struct {
	Address kv.Address
	Value   []byte
	Flags   Flags
}

type Range struct {
	Keyspace  uint32
	StartKey  []byte // inclusive
	EndKey    []byte // exclusive
	Timestamp uint64 // optional; if not specified, it represents the latest value
}

type Iterator = iter.Seq2[Entry, error]

type db struct {
	pid uint32
	db  kv.DB
}

func New(pid uint32, kvdb kv.DB) DB {
	return &db{
		pid: pid,
		db:  kvdb,
	}
}

func (d *db) Get(addr kv.Address) (*Entry, error) {
	p, err := d.getFirst(addr)
	if err != nil || p == nil {
		return nil, err
	}

	if p.Flags.Tombstone() {
		return nil, nil
	}

	return p, nil
}

func (d *db) GetRange(rang Range) (Iterator, error) {
	if rang.Timestamp == 0 {
		rang.Timestamp = math.MaxUint64
	}

	start := kv.Address{
		Keyspace:  rang.Keyspace,
		Key:       rang.StartKey,
		Timestamp: rang.Timestamp,
	}

	iter, err := d.db.GetFrom(d.pid, start)
	if err != nil {
		return nil, err
	}

	return func(yield func(Entry, error) bool) {
		var skipKey []byte

		for p, err := range iter {
			if err != nil {
				yield(Entry{}, err)
				return
			}

			value, flags := DecodeData(p.Data)

			switch {
			case p.Address.Keyspace != rang.Keyspace:
				return
			case len(rang.EndKey) > 0 && bytes.Compare(p.Address.Key, rang.EndKey) >= 0:
				return
			case flags.Tombstone():
				if p.Address.Timestamp <= rang.Timestamp {
					skipKey = p.Address.Key
				}
				continue
			case bytes.Equal(p.Address.Key, skipKey):
				continue
			case p.Address.Timestamp <= rang.Timestamp:
				entry := Entry{
					Address: p.Address,
					Value:   value,
					Flags:   flags,
				}

				if !yield(entry, nil) {
					return
				}

				skipKey = p.Address.Key
			}
		}
	}, nil
}

func (d *db) Exists(addr kv.Address) (bool, error) {
	v, err := d.Get(addr)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (d *db) ExistsInRange(rang Range) (bool, error) {
	iter, err := d.GetRange(rang)
	if err != nil {
		return false, err
	}

	for range iter {
		return true, nil
	}

	return false, nil
}

func (d *db) Put(index uint64, entries ...Entry) error {
	kvEntries := make([]kv.Entry, len(entries))

	for i, e := range entries {
		if e.Address.Timestamp == 0 {
			panic(fmt.Sprintf("trying to set key with missing timestamp at %s", e.Address))
		}

		kvEntries[i] = kv.Entry{Address: e.Address, Data: EncodeData(e.Value, e.Flags)}
	}

	return d.db.Put(d.pid, index, kvEntries...)
}

func (d *db) getFirst(addr kv.Address) (*Entry, error) {
	if addr.Timestamp == 0 {
		addr.Timestamp = math.MaxUint64
	}

	iter, err := d.db.GetFrom(d.pid, addr)
	if err != nil {
		return nil, err
	}

	for p, err := range iter {
		if err != nil {
			return nil, err
		}

		if p.Address.Keyspace != addr.Keyspace || !bytes.Equal(p.Address.Key, addr.Key) {
			return nil, nil
		} else if p.Address.Timestamp <= addr.Timestamp {
			value, flags := DecodeData(p.Data)

			entry := Entry{
				Address: p.Address,
				Value:   value,
				Flags:   flags,
			}

			return &entry, nil
		}
	}

	return nil, nil
}
