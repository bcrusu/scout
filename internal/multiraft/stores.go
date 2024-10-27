package multiraft

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"

	raftrocksdb "github.com/bcrusu/raft-rocksdb"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
)

type stores interface {
	utils.Lifecycle
	New(id uint32) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error)
	Drop(id uint32) error
}

type persistent struct {
	dataDir           string
	snapshotRetainMax int
	rocksdb           *raftrocksdb.MultiStore
	lock              sync.Mutex
	snapshot          map[uint32]*raft.FileSnapshotStore
}

func newPersistent(dataDir string, snapshotRetainMax int) stores {
	return &persistent{
		dataDir:           dataDir,
		snapshotRetainMax: snapshotRetainMax,
		snapshot:          map[uint32]*raft.FileSnapshotStore{},
	}
}

func (s *persistent) Start(ctx context.Context) error {
	rocksdbDir := path.Join(s.dataDir, "rocksdb")
	if err := utils.MkdirAll(rocksdbDir); err != nil {
		return errors.Wrapf(err, "failed to create dir %q", rocksdbDir)
	}

	s.rocksdb = raftrocksdb.NewMultiStore(
		raftrocksdb.WithPath(rocksdbDir),
	)

	return s.rocksdb.Open()
}

func (s *persistent) Stop() {
	s.rocksdb.Close()
}

func (s *persistent) New(id uint32) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	logger := newLogAdapter(fmt.Sprintf("snapshot_%d", id))
	snapshot, err := raft.NewFileSnapshotStoreWithLogger(s.snapshotDir(id), s.snapshotRetainMax, logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create snapshot store, error=%w", err)
	}

	s.lock.Lock()
	s.snapshot[id] = snapshot
	s.lock.Unlock()

	store := s.rocksdb.New(id)
	return store, store, snapshot, nil
}

func (s *persistent) Drop(id uint32) error {
	s.lock.Lock()
	delete(s.snapshot, id)
	s.lock.Unlock()

	dir := s.snapshotDir(id)
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrapf(err, "failed to remove snapshot dir %q", dir)
	}

	if err := s.rocksdb.Drop(id); err != nil {
		return errors.Wrapf(err, "failed to drop RocksDB store %d", id)
	}

	return nil
}

func (s *persistent) snapshotDir(id uint32) string {
	return path.Join(s.dataDir, "snapshot", strconv.Itoa(int(id)))
}
