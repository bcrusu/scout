package mvcc

import (
	"bytes"
	"math"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
)

// Emulated implements the MVCC features for a backing KV store.
type Emulated struct {
	db kv.DB
}

func NewEmulated(db kv.DB) *Emulated {
	return &Emulated{
		db: db,
	}
}

func (d *Emulated) Get(pid uint32, timestamp uint64, addrs ...Address) ([]*Record, error) {
	result := make([]*Record, len(addrs))

	for i, addr := range addrs {
		p, err := d.getFirst(pid, timestamp, addr)

		switch {
		case err != nil:
			return nil, err
		case p == nil || p.Flags.Tombstone():
			continue
		default:
			result[i] = p
		}
	}

	return result, nil
}

func (d *Emulated) GetRange(pid uint32, timestamp uint64, start, end Address) (Iterator, error) {
	if timestamp == 0 {
		timestamp = math.MaxUint64
	}

	kvStart := kv.NewAddress(start.Keyspace, start.Key, timestamp)
	kvEnd := kv.NewAddress(end.Keyspace, end.Key, timestamp)

	iter, err := d.db.GetRange(pid, kvStart, &kvEnd)
	if err != nil {
		return nil, err
	}

	return func(yield func(Record, error) bool) {
		var skipKey []byte

		for p, err := range iter {
			if err != nil {
				yield(Record{}, err)
				return
			}

			value, flags := DecodeData(p.Data)

			switch {
			case p.Address.Keyspace > end.Keyspace || bytes.Compare(p.Address.Key, end.Key) >= 0:
				return
			case flags.Tombstone():
				if p.Address.Timestamp <= timestamp {
					skipKey = p.Address.Key
				}
				continue
			case bytes.Equal(p.Address.Key, skipKey):
				continue
			case p.Address.Timestamp <= timestamp:
				record := Record{
					Address:   NewAddress(p.Address.Keyspace, p.Address.Key),
					Timestamp: p.Address.Timestamp,
					Value:     value,
					Flags:     flags,
				}

				if !yield(record, nil) {
					return
				}

				skipKey = p.Address.Key
			}
		}
	}, nil
}

func (d *Emulated) Exists(pid uint32, timestamp uint64, addr Address) (bool, error) {
	v, err := d.Get(pid, timestamp, addr)
	if err != nil {
		return false, err
	}
	return v[0] != nil, nil
}

func (d *Emulated) ExistsInRange(pid uint32, timestamp uint64, start, end Address) (bool, error) {
	iter, err := d.GetRange(pid, timestamp, start, end)
	if err != nil {
		return false, err
	}

	for range iter {
		return true, nil
	}

	return false, nil
}

func (d *Emulated) Put(pid uint32, index uint64, records ...Record) error {
	kvRecords := make([]kv.Record, len(records))

	for i, e := range records {
		if e.Timestamp == 0 {
			return errors.Errorf("trying to put record with missing timestamp at %s", e.Address)
		}

		addr := kv.NewAddress(e.Address.Keyspace, e.Address.Key, e.Timestamp)
		data := EncodeData(e.Value, e.Flags)

		kvRecords[i] = kv.Record{Address: addr, Data: data}
	}

	return d.db.Put(pid, index, kvRecords...)
}

func (d *Emulated) getFirst(pid uint32, timestamp uint64, addr Address) (*Record, error) {
	if timestamp == 0 {
		timestamp = math.MaxUint64
	}

	start := kv.NewAddress(addr.Keyspace, addr.Key, timestamp)
	end := kv.LastAddressForKey(addr.Keyspace, addr.Key).Next() // end is exclusive

	iter, err := d.db.GetRange(pid, start, &end)
	if err != nil {
		return nil, err
	}

	for p, err := range iter {
		if err != nil {
			return nil, err
		}

		addr := NewAddress(p.Address.Keyspace, p.Address.Key)
		value, flags := DecodeData(p.Data)

		return &Record{
			Address:   addr,
			Timestamp: p.Address.Timestamp,
			Value:     value,
			Flags:     flags,
		}, nil
	}

	return nil, nil
}
