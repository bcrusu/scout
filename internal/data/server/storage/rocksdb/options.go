package rocksdb

import (
	"path"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/linxGnu/grocksdb"
)

var (
	comparatorName    = "scout"
	mergeOperatorName = "scout"
)

func makeDBOptions(config config.RocksDBConfig) *grocksdb.Options {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(config.CacheSize.MustParse()))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissingColumnFamilies(false)
	opts.SetCreateIfMissing(true)
	opts.SetComparator(grocksdb.NewComparator(comparatorName, kv.CompareKeys))
	opts.SetManualWALFlush(true) // disable all wall knobs, see below

	return opts
}

func makeCFOptions(config config.RocksDBConfig, name string) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.SetComparator(grocksdb.NewComparator(comparatorName, kv.CompareKeys))
	opts.SetMergeOperator(&mergeOperator{})
	// opts.SetPrefixExtractor() // todo

	if name == cfDefaultName {
		return opts
	}

	paths := []*grocksdb.DBPath{
		// using an arbitrarily large value here just to force each cf to
		// use a separate directory.
		grocksdb.NewDBPath(path.Join(config.DataDir, name), 1<<40),
	}

	opts.SetCFPaths(paths)

	return opts
}

func makeCFsOptions(config config.RocksDBConfig, names ...string) []*grocksdb.Options {
	result := make([]*grocksdb.Options, len(names))
	for i, name := range names {
		result[i] = makeCFOptions(config, name)
	}

	return result
}

func makeWriteOptions() *grocksdb.WriteOptions {
	opts := grocksdb.NewDefaultWriteOptions()

	// Contents are already persisted in the raft log: will skip writing again
	// to RocksDB wal, but it is paramout to first flush-and-check RocksDB
	// before performing a Raft snapshot. Handled in the Raft FSM.
	opts.DisableWAL(true)

	return opts
}

func makeReadOptions() *grocksdb.ReadOptions {
	opts := grocksdb.NewDefaultReadOptions()
	//SetFillCache TODO

	return opts
}
