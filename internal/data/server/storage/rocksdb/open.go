package rocksdb

import (
	"path"

	"github.com/bcrusu/graph/internal/data/server/config"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/linxGnu/grocksdb"
)

const (
	currentFileName = "CURRENT" // defined in rocksdb/file/filename.cc
)

func openDB(config config.RocksDBConfig) (*grocksdb.DB, cfMap, error) {
	currentFilePath := path.Join(config.DataDir, currentFileName)
	exists, err := utils.PathExists(currentFilePath)
	if err != nil {
		return nil, nil, err
	}

	opts := makeDBOptions(config)

	if !exists {
		db, err := grocksdb.OpenDb(opts, config.DataDir)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to open db.")
		}
		return db, cfMap{}, nil
	}

	cfNames, err := grocksdb.ListColumnFamilies(opts, config.DataDir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to list column families.")
	}

	cfOpts := makeCFsOptions(config, cfNames...)

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, config.DataDir, cfNames, cfOpts)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open db column families.")
	}

	known, unknown, err := probeCFs(db, cfHandles)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to probe column families.")
	}

	for _, h := range unknown {
		h.Destroy() // release unknown cfs
	}

	return db, known, nil
}
