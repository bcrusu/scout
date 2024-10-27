package multiraft

import (
	"context"
	"sync"

	"github.com/hashicorp/raft"
)

type inmem struct {
	lock     sync.Mutex
	store    map[uint32]*raft.InmemStore
	snapshot map[uint32]*raft.InmemSnapshotStore
}

func newInmem() stores {
	return &inmem{
		store:    map[uint32]*raft.InmemStore{},
		snapshot: map[uint32]*raft.InmemSnapshotStore{},
	}
}

func (r *inmem) Start(ctx context.Context) error {
	return nil
}

func (r *inmem) Stop() {}

func (s *inmem) New(id uint32) (raft.LogStore, raft.StableStore, raft.SnapshotStore, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	store := raft.NewInmemStore()
	snapshot := raft.NewInmemSnapshotStore()

	s.store[id] = store
	s.snapshot[id] = snapshot
	return store, store, snapshot, nil
}

func (s *inmem) Drop(id uint32) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.store, id)
	delete(s.snapshot, id)
	return nil
}
