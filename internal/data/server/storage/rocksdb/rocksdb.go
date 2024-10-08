package rocksdb

import (
	"context"
	"sync/atomic"

	"github.com/bcrusu/graph/internal/data/server/config"
	"github.com/bcrusu/graph/internal/data/server/storage/kv"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/linxGnu/grocksdb"
)

var (
	_   kv.DB           = (*RocksDB)(nil)
	_   utils.Lifecycle = (*RocksDB)(nil)
	log                 = logging.WithComponent("rocksdb").NoContext()
)

type RocksDB struct {
	config config.RocksDBConfig
	db     *grocksdb.DB
	ro     *grocksdb.ReadOptions
	wo     *grocksdb.WriteOptions
	cfs    atomic.Pointer[cfMap]
}

func NewRocksDB() *RocksDB {
	return &RocksDB{
		config: config.Get().RocksDB,
	}
}

func (r *RocksDB) Start(ctx context.Context) error {
	db, cfs, err := openDB(r.config)
	if err != nil {
		return err
	}

	log.Debug("RocksDB started.", "partitions", utils.MakeKeySlice(cfs))

	r.db = db
	r.cfs.Store(&cfs)
	r.ro = makeReadOptions()
	r.wo = makeWriteOptions()
	return nil
}

func (r *RocksDB) Stop() {
	for pid, cf := range r.getCFMap() {
		// flush happens during destroy, but does not hurt to be safe
		r.flushCF(pid, cf)
		cf.Destroy()
	}

	r.ro.Destroy()
	r.wo.Destroy()
	r.db.Close()
}

func (r *RocksDB) InitPartition(pid uint32) error {
	if r.getCF(pid) != nil {
		return errors.AlreadyExists
	}

	name := getCFName(pid)
	opts := makeCFOptions(r.config, name)

	cf, err := r.db.CreateColumnFamily(opts, name)
	if err != nil {
		return errors.Wrapf(err, "failed to create column family %s.", name)
	}

	if err := initCF(r.db, cf, pid); err != nil {
		return errors.Wrapf(err, "failed to init column family %s.", name)
	}

	new := cfMap(utils.CloneMap(r.getCFMap()))
	new[pid] = cf
	r.cfs.Store(&new)
	return nil
}

func (r *RocksDB) DropPartition(pid uint32) error {
	cf := r.getCF(pid)
	if cf == nil {
		return errors.NotFound
	}

	if err := r.db.DropColumnFamily(cf); err != nil {
		return errors.Wrapf(err, "failed to drop column family for pid=%d", pid)
	}

	new := cfMap(utils.CloneMap(r.getCFMap()))
	delete(new, pid)
	r.cfs.Store(&new)

	cf.Destroy()
	return nil
}

func (r *RocksDB) SyncPartition(pid uint32) (uint64, error) {
	cf := r.getCF(pid)
	if cf == nil {
		return 0, errors.NotFound
	}

	if err := r.flushCF(pid, cf); err != nil {
		return 0, err
	}

	// read persisted index to confirm latest on-disk version
	index, err := readCFIndex(r.db, cf, grocksdb.PersistedTier)
	if err != nil && !errors.Is(err, errors.NotFound) {
		return 0, errors.Wrap(err, "failed to read persisted index.")
	}

	return index, nil
}

func (r *RocksDB) Put(index uint64, pid uint32, entries ...kv.Entry) error {
	cf := r.getCF(pid)
	if cf == nil {
		return errors.NotFound
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, entry := range entries {
		key := kv.EncodeKey(entry.Address, entry.Flags)
		value := entry.Value
		batch.PutCF(cf, key, value)
	}

	// Deduplication heppens in memtable before sst flush and during compactions.
	// Does not need any kind of locking as the max value logic is handled
	// inside the merge operator.
	batch.MergeCF(cf, keyIndex, encodeUint64(index))

	return r.db.Write(r.wo, batch)
}

func (r *RocksDB) Get(pid uint32, start kv.Address) (kv.Iterator, error) {
	cf := r.getCF(pid)
	if cf == nil {
		return nil, errors.NotFound
	}

	return func(yield func(kv.Entry, error) bool) {
		it := r.db.NewIteratorCF(r.ro, cf)
		defer it.Close()

		it.Seek(kv.EncodeKey(start, 0))

		for ; it.Valid(); it.Next() {
			key := it.Key()
			value := it.Value()

			addr, flags := kv.DecodeKey(key.Data())

			x := kv.Entry{
				Address: addr,
				Value:   value.Data(),
				Flags:   flags,
			}

			key.Free()
			value.Free()

			if !yield(x, nil) {
				return
			}
		}

		if err := it.Err(); err != nil {
			yield(kv.Entry{}, err)
		}
	}, nil
}

func (r *RocksDB) getCF(pid uint32) *grocksdb.ColumnFamilyHandle {
	x := r.cfs.Load()
	if x == nil {
		return nil
	}
	return (*x)[pid]
}

func (r *RocksDB) getCFMap() cfMap {
	x := r.cfs.Load()
	if x == nil {
		return cfMap{}
	}
	return *x
}

func (r *RocksDB) flushCF(pid uint32, cf *grocksdb.ColumnFamilyHandle) error {
	opts := grocksdb.NewDefaultFlushOptions()
	opts.SetWait(true)
	defer opts.Destroy()

	if err := r.db.FlushCF(cf, opts); err != nil {
		return errors.Wrapf(err, "failed to flush column family for pid=%d", pid)
	}

	return nil
}
