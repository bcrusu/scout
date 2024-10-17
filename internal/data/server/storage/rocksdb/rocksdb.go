package rocksdb

import (
	"context"
	"sync/atomic"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/linxGnu/grocksdb"
)

var (
	_   storage.DB      = (*RocksDB)(nil)
	_   utils.Lifecycle = (*RocksDB)(nil)
	log                 = logging.WithComponent("rocksdb").NoContext()
)

type RocksDB struct {
	config config.RocksDB
	kv     *rocksKV
	mvcc   *rocksMVCC
	db     *grocksdb.DB
	wo     *grocksdb.WriteOptions
	cfs    atomic.Pointer[cfMap]
}

func NewWithConfig(config config.RocksDB) *RocksDB {
	return &RocksDB{
		config: config,
	}
}

func New() *RocksDB {
	return NewWithConfig(config.Get().DB.RocksDB)
}

func (r *RocksDB) Start(ctx context.Context) error {
	db, cfs, err := openDB(r.config)
	if err != nil {
		return err
	}

	log.Debug("RocksDB started.", "partitions", utils.MakeKeySlice(cfs))

	r.db = db
	r.cfs.Store(&cfs)
	r.wo = makeWriteOptions()
	r.kv = newRocksKV(r)
	r.mvcc = newRocksMVCC(r)
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

	r.wo.Destroy()
	r.db.Close()
}

func (r *RocksDB) KV() kv.DB {
	return r.kv
}

func (r *RocksDB) MVCC() mvcc.DB {
	return r.mvcc
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
