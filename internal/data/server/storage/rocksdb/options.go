package rocksdb

import (
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/linxGnu/grocksdb"
)

const (
	extensionName = "scout"
)

func makeDBOptions(config config.RocksDB) *grocksdb.Options {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(config.CacheSize.MustParse()))
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(config.BloomFilterBitsPerKey))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissingColumnFamilies(false)
	opts.SetCreateIfMissing(true)
	opts.SetComparator(newComparator())
	opts.SetManualWALFlush(true) // disable all wall knobs, see below

	return opts
}

func makeCFOptions(config config.RocksDB, name string) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.SetComparator(newComparator())
	opts.SetMergeOperator(&mergeOperator{})
	opts.SetWriteBufferSize(config.WriteBufferSize.MustParse())
	opts.SetPrefixExtractor(grocksdb.NewCappedPrefixTransform(config.MaxKeyPrefixLen))

	if name == cfDefaultName {
		return opts
	}

	paths := []*grocksdb.DBPath{
		// using an arbitrarily large value here just to force each cf to
		// use a separate directory.
		grocksdb.NewDBPath(getCFPath(config.DataDir, name), 1<<40),
	}

	opts.SetCFPaths(paths)

	return opts
}

func makeCFsOptions(config config.RocksDB, names ...string) []*grocksdb.Options {
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
	// before performing a Raft snapshot. This is handled in the Raft FSM.
	opts.DisableWAL(true)

	return opts
}

func makeReadOptionsMVCC() *grocksdb.ReadOptions {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetAutoPrefixMode(true)

	return opts
}

func makeReadOptionsKV() *grocksdb.ReadOptions {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetAutoPrefixMode(true)
	opts.SetIterStartTimestamp(minUint64)
	opts.SetTimestamp(maxUint64)

	return opts
}

func makeStreamOptions(config config.RocksDB) *grocksdb.ReadOptions {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	opts.SetTotalOrderSeek(true)
	opts.SetAsyncIO(true)
	opts.SetReadaheadSize(config.MaxReadaheadSize.MustParse())
	opts.SetIterStartTimestamp(minUint64)
	opts.SetTimestamp(maxUint64)

	return opts
}
