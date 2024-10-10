package inmem

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
)

var (
	_ kv.DB = (*DB)(nil)
)

type DB struct {
	partitions map[uint32]*partition
}

type partition struct {
	index uint64
	items []item
}

type item struct {
	key  []byte
	data []byte
}

func New() *DB {
	return &DB{
		partitions: map[uint32]*partition{},
	}
}

func (d *DB) InitPartition(pid uint32) error {
	return nil
}

func (d *DB) DropPartition(pid uint32) error {
	return nil
}

func (d *DB) SyncPartition(pid uint32) error {
	return nil
}

func (d *DB) GetIndex(pid uint32, _ bool) (uint64, error) {
	part := d.getPartition(pid)
	return part.index, nil
}

func (d *DB) Get(pid uint32, address kv.Address) (*kv.Entry, error) {
	part := d.getPartition(pid)
	key := address.Encode()
	i, found := part.findFirst(key)
	if !found {
		return nil, nil
	}

	a := part.items[i]

	return &kv.Entry{
		Address: address,
		Data:    a.data,
	}, nil
}

func (d *DB) GetFrom(pid uint32, start kv.Address) (kv.Iterator, error) {
	part := d.getPartition(pid)
	startKey := start.Encode()
	startIdx, _ := part.findFirst(startKey)

	return func(yield func(kv.Entry, error) bool) {
		for i := startIdx; i < part.Len(); i++ {
			a := part.items[i]

			x := kv.Entry{
				Address: kv.DecodeAddress(a.key),
				Data:    a.data,
			}

			if !yield(x, nil) {
				return
			}
		}
	}, nil
}

func (d *DB) GetStream(pid uint32, start kv.Address) (kv.Iterator, error) {
	return d.GetFrom(pid, start)
}

func (d *DB) Put(pid uint32, index uint64, entries ...kv.Entry) error {
	part := d.getPartition(pid)

	for _, p := range entries {
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

func (d *DB) putOne(part *partition, entry kv.Entry) error {
	key := entry.Address.Encode()

	i, found := part.findFirst(key)
	if found {
		// allow only idempotent operations to keep the MVCC entries immutable.
		if !bytes.Equal(entry.Data, part.items[i].data) {
			panic("key overwrite detected")
		}
		return nil
	}

	part.items = append(part.items, item{key, entry.Data})
	return nil
}

func (d *DB) getPartition(pid uint32) *partition {
	part, ok := d.partitions[pid]
	if !ok {
		part = &partition{}
		d.partitions[pid] = part
	}

	return part
}

func (p *partition) Len() int {
	return len(p.items)
}

func (p *partition) Less(i, j int) bool {
	a := p.items[i]
	b := p.items[j]
	return kv.CompareKeys(a.key, b.key) < 0
}

func (p *partition) Swap(i, j int) {
	p.items[i], p.items[j] = p.items[j], p.items[i]
}

func (p *partition) findFirst(key []byte) (int, bool) {
	return sort.Find(len(p.items), func(i int) int {
		a := p.items[i]
		return kv.CompareKeys(key, a.key)
	})
}
