package inmem

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/bcrusu/graph/internal/data/server/storage"
)

var (
	_ storage.DB = (*inmemDB)(nil)
)

type inmemDB struct {
	partitions map[uint32]*partition
}

type partition struct {
	keyspaces map[uint64]*keyspace
}

type keyspace struct {
	entries []*entry
}

type entry struct {
	key       []byte
	timestamp uint64
	data      []byte
	tombstone bool
}

func New() storage.DB {
	return &inmemDB{
		partitions: map[uint32]*partition{},
	}
}

func (d *inmemDB) Get(loc storage.Location) (*storage.ValueAt, error) {
	ks := d.getKeyspace(loc.PartitionID, loc.Keyspace)

	i, found := ks.findKeyAt(loc.Key, loc.Timestamp)
	if !found || ks.entries[i].tombstone {
		return nil, nil
	}

	return &storage.ValueAt{
		Data:      ks.entries[i].data,
		Timestamp: ks.entries[i].timestamp,
	}, nil
}

func (d *inmemDB) GetRange(rang storage.Range) (storage.Iterator, error) {
	ks := d.getKeyspace(rang.PartitionID, rang.Keyspace)

	startIdx := ks.findFirst(rang.StartKey, rang.Timestamp)
	if startIdx == ks.Len() {
		return emptyRange, nil
	}

	return func(yield func(storage.ValueAt, error) bool) {
		var skipKey []byte

		for i := startIdx; i < ks.Len(); i++ {
			a := ks.entries[i]

			switch {
			case bytes.Equal(a.key, rang.EndKey):
				return
			case a.tombstone:
				if a.timestamp <= rang.Timestamp {
					skipKey = a.key
				}
				continue
			case bytes.Equal(a.key, skipKey):
				// pass
			case rang.Timestamp == 0:
				if !yield(storage.ValueAt{Data: a.data, Timestamp: a.timestamp}, nil) {
					return
				}
				skipKey = a.key
			case a.timestamp <= rang.Timestamp:
				if !yield(storage.ValueAt{Data: a.data, Timestamp: a.timestamp}, nil) {
					return
				}
				skipKey = a.key
			}
		}
	}, nil
}

func (d *inmemDB) Exists(loc storage.Location) (bool, error) {
	v, err := d.Get(loc)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (d *inmemDB) ExistsInRange(rang storage.Range) (bool, error) {
	iter, err := d.GetRange(rang)
	if err != nil {
		return false, err
	}

	for range iter {
		return true, nil
	}

	return false, nil
}

func (d *inmemDB) Set(loc storage.Location, data []byte) error {
	if loc.Timestamp == 0 {
		panic(fmt.Sprintf("trying to set key with missing timestamp at location=%s", loc))
	}

	ks := d.getKeyspace(loc.PartitionID, loc.Keyspace)

	i, found := ks.findKeyAt(loc.Key, loc.Timestamp)
	if found && ks.entries[i].timestamp == loc.Timestamp {
		// allow only idempotent operations to keep the MVCC entries immutable.
		if !bytes.Equal(data, ks.entries[i].data) {
			panic(fmt.Sprintf("overwrite detected at %s", loc))
		}
		return nil
	}

	entry := &entry{
		key:       loc.Key,
		timestamp: loc.Timestamp,
		data:      data,
		tombstone: false,
	}

	ks.entries = append(ks.entries, entry)
	sort.Sort(ks)
	return nil
}

func (d *inmemDB) Delete(loc storage.Location) error {
	if loc.Timestamp == 0 {
		panic(fmt.Sprintf("trying to set key with missing timestamp at location=%s", loc))
	}

	ks := d.getKeyspace(loc.PartitionID, loc.Keyspace)

	i, found := ks.findKeyAt(loc.Key, loc.Timestamp)
	if found && ks.entries[i].timestamp == loc.Timestamp {
		// allow only idempotent operations to keep the MVCC entries immutable.
		if !ks.entries[i].tombstone {
			panic(fmt.Sprintf("overwrite detected at %s", loc))
		}
		return nil
	}

	entry := &entry{
		key:       loc.Key,
		timestamp: loc.Timestamp,
		tombstone: true,
	}

	ks.entries = append(ks.entries, entry)
	sort.Sort(ks)
	return nil
}

func (d *inmemDB) getKeyspace(pid uint32, ks uint64) *keyspace {
	part, ok := d.partitions[pid]
	if !ok {
		part = &partition{keyspaces: map[uint64]*keyspace{}}
		d.partitions[pid] = part
	}

	if _, ok := part.keyspaces[ks]; !ok {
		part.keyspaces[ks] = &keyspace{}
	}

	return part.keyspaces[ks]
}

func (k *keyspace) Len() int {
	return len(k.entries)
}

// entries are sorted asc by key and desc by timestamp, resulting in
// a sort order with latest version at the start of each key range.
func (k *keyspace) Less(i, j int) bool {
	a := k.entries[i]
	b := k.entries[j]

	if x := bytes.Compare(a.key, b.key); x < 0 {
		return true
	} else if x > 0 {
		return false
	} else {
		return a.timestamp > b.timestamp
	}
}

func (k *keyspace) Swap(i, j int) {
	k.entries[i], k.entries[j] = k.entries[j], k.entries[i]
}

// findKeyAt returns the index of the latest entry up to the timestamp.
// The optional timestamp == 0 represents the latest key entry.
func (k *keyspace) findKeyAt(key []byte, timestamp uint64) (int, bool) {
	i := k.findFirst(key, timestamp)
	if i < len(k.entries) && bytes.Equal(key, k.entries[i].key) {
		return i, true
	}
	return -1, false
}

func (k *keyspace) findFirst(key []byte, timestamp uint64) int {
	i, _ := sort.Find(len(k.entries), func(i int) int {
		a := k.entries[i]
		if x := bytes.Compare(key, a.key); x != 0 {
			return x
		} else if timestamp == 0 {
			return 0
		} else {
			return int(a.timestamp - timestamp)
		}
	})

	return i
}

func emptyRange(yield func(storage.ValueAt, error) bool) {}
