package rocksdb

import (
	"os"
	"slices"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/linxGnu/grocksdb"
)

var (
	_ kv.DB = (*rocksKV)(nil)
)

type rocksKV struct {
	rdb *RocksDB
}

func newRocksKV(rdb *RocksDB) *rocksKV {
	return &rocksKV{
		rdb: rdb,
	}
}

func (r *rocksKV) InitPartition(pid uint32) error {
	if r.rdb.getCF(pid) != nil {
		return nil
	}

	name := getCFName(pid)
	opts := makeCFOptions(r.rdb.config, name)

	cf, err := r.rdb.db.CreateColumnFamily(opts, name)
	if err != nil {
		return errors.Wrapf(err, "failed to create column family %s.", name)
	}

	if err := initCF(r.rdb.db, cf, pid); err != nil {
		return errors.Wrapf(err, "failed to init column family %s.", name)
	}

	new := cfMap(utils.CloneMap(r.rdb.getCFMap()))
	new[pid] = cf
	r.rdb.cfs.Store(&new)
	return nil
}

func (r *rocksKV) DropPartition(pid uint32) error {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return errors.NotFound
	}

	new := cfMap(utils.CloneMap(r.rdb.getCFMap()))
	delete(new, pid)
	r.rdb.cfs.Store(&new)

	if err := r.rdb.db.DropColumnFamily(cf); err != nil {
		return errors.Wrapf(err, "failed to drop column family for partition=%d", pid)
	}

	cf.Destroy()

	name := getCFName(pid)
	path := getCFPath(r.rdb.config.DataDir, name)

	if err := os.Remove(path); err != nil {
		log.WithError(err).Error("Failed to remove column family directory.", "dir", path)
	}

	return nil
}

func (r *rocksKV) SyncPartition(pid uint32) error {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return errors.NotFound
	}

	if err := flushCF(r.rdb.db, cf); err != nil {
		return err
	}

	return nil
}

func (r *rocksKV) Put(pid uint32, index uint64, records ...kv.Record) error {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return errors.NotFound
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, record := range records {
		key := r.encodeKey(record.Address)
		ts := encodeUint64(record.Address.Timestamp)
		batch.PutCFWithTS(cf, key, ts, record.Data)
	}

	// Deduplication happens in memtable before SST flush and during compactions.
	// Does not need any kind of locking as the max value logic is handled
	// inside the merge operator. Will write even when records slice is empty which
	// allows the FSM to just bump the index.
	if index > 0 {
		batch.MergeCFWithTS(cf, keyIndex, minUint64, encodeUint64(index))
	}

	return r.rdb.db.Write(r.rdb.wo, batch)
}

func (r *rocksKV) Get(pid uint32, address kv.Address) (*kv.Record, error) {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return nil, errors.NotFound
	}

	ro := makeReadOptionsKV()
	ro.SetTimestamp(encodeUint64(address.Timestamp))
	defer ro.Destroy()

	value, timestamp, err := r.rdb.db.GetCFWithTS(ro, cf, r.encodeKey(address))
	if err != nil {
		return nil, err
	}
	defer value.Free()
	defer timestamp.Free()

	if !value.Exists() {
		return nil, nil
	}

	ts := errors.Assert2(decodeUint64(timestamp.Data()))
	if ts != address.Timestamp {
		return nil, nil
	}

	return &kv.Record{
		Address: address,
		Data:    slices.Clone(value.Data()),
	}, nil
}

func (r *rocksKV) GetRange(pid uint32, start kv.Address, end *kv.Address) (kv.Iterator, error) {
	ro := makeReadOptionsKV()
	return r.getIterator(pid, ro, start, end)
}

func (r *rocksKV) GetStream(pid uint32, start kv.Address) (kv.Iterator, error) {
	ro := makeStreamOptions(r.rdb.config)
	return r.getIterator(pid, ro, start, nil)
}

func (r *rocksKV) getIterator(pid uint32, ro *grocksdb.ReadOptions, start kv.Address, end *kv.Address) (kv.Iterator, error) {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return nil, errors.NotFound
	}

	startKey := r.encodeKey(start)

	return func(yield func(kv.Record, error) bool) {
		defer ro.Destroy()

		if end != nil {
			next := end.NextKey() // need to next because upper bound is exclusive
			endKey := r.encodeKey(next)
			ro.SetIterateUpperBound(endKey)
		}

		it := r.rdb.db.NewIteratorCF(ro, cf)
		defer it.Close()

		it.Seek(startKey)

		for ; it.Valid(); it.Next() {
			addr := r.decodeAddressFromIter(it)
			if addr.Before(start) {
				continue
			} else if end != nil && addr.Compare(*end) >= 0 {
				return
			}

			value := it.Value()

			e := kv.Record{
				Address: addr,
				Data:    slices.Clone(value.Data()),
			}

			value.Free()

			if !yield(e, nil) {
				return
			}
		}

		if err := it.Err(); err != nil {
			yield(kv.Record{}, err)
		}
	}, nil
}

func (r *rocksKV) GetIndex(pid uint32, persistedOnDisk bool) (uint64, error) {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return 0, errors.NotFound
	}

	readTier := grocksdb.ReadAllTier
	if persistedOnDisk {
		readTier = grocksdb.PersistedTier
	}

	index, err := readCFIndex(r.rdb.db, cf, readTier)
	if err != nil && !errors.Is(err, errors.NotFound) {
		return 0, errors.Wrap(err, "failed to read persisted index.")
	}

	return index, nil
}

func (r *rocksKV) encodeKey(a kv.Address) []byte {
	return encodeKey(a.Keyspace, a.Key)
}

func (r *rocksKV) decodeAddressFromIter(iter *grocksdb.Iterator) kv.Address {
	return r.decodeAddress(iter.Key(), iter.Timestamp())
}

func (r *rocksKV) decodeAddress(keySlice, timestampSlice *grocksdb.Slice) kv.Address {
	keyspace, key := decodeKey(keySlice.Data())
	ts := errors.Assert2(decodeUint64(timestampSlice.Data()))
	addr := kv.NewAddress(keyspace, key, ts)

	keySlice.Free()
	timestampSlice.Free()
	return addr
}
