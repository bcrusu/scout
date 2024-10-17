package inmem

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
)

var (
	_ kv.DB = (*DB)(nil)
)

type DB struct {
	partitions map[uint32]*partition
}

type partition struct {
	index   uint64
	records []kv.Record
}

func New() *DB {
	return &DB{
		partitions: map[uint32]*partition{},
	}
}

func (d *DB) InitPartition(pid uint32) error {
	_, ok := d.partitions[pid]
	if ok {
		return nil
	}

	d.partitions[pid] = &partition{}
	return nil
}

func (d *DB) DropPartition(pid uint32) error {
	delete(d.partitions, pid)
	return nil
}

func (d *DB) SyncPartition(pid uint32) error {
	return nil
}

func (d *DB) GetIndex(pid uint32, _ bool) (uint64, error) {
	part, ok := d.partitions[pid]
	if !ok {
		return 0, errors.NotFound
	}

	return part.index, nil
}

func (d *DB) Get(pid uint32, address kv.Address) (*kv.Record, error) {
	part, ok := d.partitions[pid]
	if !ok {
		return nil, errors.NotFound
	}

	i, found := part.findFirst(address)
	if !found {
		return nil, nil
	}

	return &part.records[i], nil
}

func (d *DB) GetRange(pid uint32, start kv.Address, end *kv.Address) (kv.Iterator, error) {
	part, ok := d.partitions[pid]
	if !ok {
		return nil, errors.NotFound
	}

	startIdx, _ := part.findFirst(start)

	return func(yield func(kv.Record, error) bool) {
		for i := startIdx; i < part.Len(); i++ {
			a := part.records[i]
			if end != nil && a.Address.Compare(*end) >= 0 {
				return
			}

			if !yield(a, nil) {
				return
			}
		}
	}, nil
}

func (d *DB) GetStream(pid uint32, start kv.Address) (kv.Iterator, error) {
	return d.GetRange(pid, start, nil)
}

func (d *DB) Put(pid uint32, index uint64, records ...kv.Record) error {
	part, ok := d.partitions[pid]
	if !ok {
		return errors.NotFound
	}

	for _, p := range records {
		if p.Address.Timestamp == 0 {
			panic(fmt.Sprintf("trying to put key with missing timestamp at %s", p.Address))
		}

		if err := d.putOne(part, p); err != nil {
			return err
		}
	}

	part.index = max(part.index, index)
	sort.Sort(part)
	return nil
}

func (d *DB) putOne(part *partition, record kv.Record) error {
	i, found := part.findFirst(record.Address)
	if found {
		// allow only idempotent operations to keep the MVCC records immutable.
		if !bytes.Equal(record.Data, part.records[i].Data) {
			panic("key overwrite detected")
		}
		return nil
	}

	part.records = append(part.records, record)
	return nil
}

func (p *partition) Len() int {
	return len(p.records)
}

func (p *partition) Less(i, j int) bool {
	a := p.records[i]
	b := p.records[j]
	return a.Address.Before(b.Address)
}

func (p *partition) Swap(i, j int) {
	p.records[i], p.records[j] = p.records[j], p.records[i]
}

func (p *partition) findFirst(addr kv.Address) (int, bool) {
	return sort.Find(len(p.records), func(i int) int {
		a := p.records[i]
		return addr.Compare(a.Address)
	})
}
