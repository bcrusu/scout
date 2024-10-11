package rocksdb

import (
	"context"
	"os"
	"sync/atomic"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/linxGnu/grocksdb"
)

var (
	_   kv.DB           = (*RocksDB)(nil)
	_   utils.Lifecycle = (*RocksDB)(nil)
	log                 = logging.WithComponent("rocksdb").NoContext()
)

type RocksDB struct {
	config config.RocksDB
	db     *grocksdb.DB
	ro     *grocksdb.ReadOptions
	so     *grocksdb.ReadOptions
	wo     *grocksdb.WriteOptions
	cfs    atomic.Pointer[cfMap]
}

func NewRocksDB() *RocksDB {
	return &RocksDB{
		config: config.Get().DB.RocksDB,
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
	r.so = makeStreamOptions(r.config)
	r.wo = makeWriteOptions()
	return nil
}

func (r *RocksDB) Stop() {
	for pid, cf := range r.getCFMap() {
		// flush happens during destroy, but does not hurt to be safe
		if err := flushCF(r.db, cf); err != nil {
			log.WithError(err).Error("Failed to flush column family.", "partition", pid)
		}

		cf.Destroy()
	}

	r.ro.Destroy()
	r.so.Destroy()
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

	new := cfMap(utils.CloneMap(r.getCFMap()))
	delete(new, pid)
	r.cfs.Store(&new)

	if err := r.db.DropColumnFamily(cf); err != nil {
		return errors.Wrapf(err, "failed to drop column family for partition=%d", pid)
	}

	cf.Destroy()

	name := getCFName(pid)
	path := getCFPath(r.config.DataDir, name)

	if err := os.Remove(path); err != nil {
		log.WithError(err).Error("Failed to remove column family directory.", "dir", path)
	}

	return nil
}

func (r *RocksDB) SyncPartition(pid uint32) error {
	cf := r.getCF(pid)
	if cf == nil {
		return errors.NotFound
	}

	if err := flushCF(r.db, cf); err != nil {
		return err
	}

	return nil
}

func (r *RocksDB) Put(pid uint32, index uint64, entries ...kv.Entry) error {
	cf := r.getCF(pid)
	if cf == nil {
		return errors.NotFound
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, entry := range entries {
		key := entry.Address.Encode()
		batch.PutCF(cf, key, entry.Data)
	}

	// Deduplication heppens in memtable before SST flush and during compactions.
	// Does not need any kind of locking as the max value logic is handled
	// inside the merge operator. Will write even when entries slice is empty which
	// allows the FSM to just bump the index.
	if index > 0 {
		batch.MergeCF(cf, keyIndex, encodeUint64(index))
	}

	return r.db.Write(r.wo, batch)
}

func (r *RocksDB) Get(pid uint32, address kv.Address) (*kv.Entry, error) {
	cf := r.getCF(pid)
	if cf == nil {
		return nil, errors.NotFound
	}

	slice, err := r.db.GetCF(r.ro, cf, address.Encode())
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, nil
	}

	return &kv.Entry{
		Address: address,
		Data:    slice.Data(),
	}, nil
}

func (r *RocksDB) GetFrom(pid uint32, start kv.Address) (kv.Iterator, error) {
	return r.getWithOptions(pid, start, r.ro)
}

func (r *RocksDB) GetStream(pid uint32, start kv.Address) (kv.Iterator, error) {
	return r.getWithOptions(pid, start, r.so)
}

func (r *RocksDB) getWithOptions(pid uint32, start kv.Address, ro *grocksdb.ReadOptions) (kv.Iterator, error) {
	cf := r.getCF(pid)
	if cf == nil {
		return nil, errors.NotFound
	}

	return func(yield func(kv.Entry, error) bool) {
		it := r.db.NewIteratorCF(ro, cf)
		defer it.Close()

		it.Seek(start.Encode())

		for ; it.Valid(); it.Next() {
			key := it.Key()
			value := it.Value()

			x := kv.Entry{
				Address: kv.DecodeAddress(key.Data()),
				Data:    value.Data(),
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

		it.Close()
	}, nil
}

func (r *RocksDB) GetIndex(pid uint32, persistedOnDisk bool) (uint64, error) {
	cf := r.getCF(pid)
	if cf == nil {
		return 0, errors.NotFound
	}

	readTier := grocksdb.ReadAllTier
	if persistedOnDisk {
		readTier = grocksdb.PersistedTier
	}

	index, err := readCFIndex(r.db, cf, readTier)
	if err != nil && !errors.Is(err, errors.NotFound) {
		return 0, errors.Wrap(err, "failed to read persisted index.")
	}

	return index, nil
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
