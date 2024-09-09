package multiraft

import (
	"io"

	"github.com/hashicorp/raft"
)

var (
	_ raft.LogStore      = (*LogStore)(nil)
	_ raft.StableStore   = (*StableStore)(nil)
	_ raft.SnapshotStore = (*SnapshotStore)(nil)
)

type LogStore struct {
	inmem raft.LogStore
}

type StableStore struct {
	inmem raft.StableStore
}

type SnapshotStore struct {
	inmem raft.SnapshotStore
}

func NewLogStore() *LogStore {
	return &LogStore{
		inmem: raft.NewInmemStore(),
	}
}

func NewStableStore() *StableStore {
	return &StableStore{
		inmem: raft.NewInmemStore(),
	}
}

func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{
		inmem: raft.NewInmemSnapshotStore(),
	}
}

// FirstIndex returns the first index written. 0 for no entries.
func (s *LogStore) FirstIndex() (uint64, error) {
	i, err := s.inmem.FirstIndex()
	if err != nil {
		log.WithError(err).Trace("LogStore.FirstIndex failed")
		return 0, err
	}

	log.Trace("LogStore.FirstIndex", "index", i)
	return i, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (s *LogStore) LastIndex() (uint64, error) {
	i, err := s.inmem.LastIndex()
	if err != nil {
		log.WithError(err).Trace("LogStore.LastIndex failed")
		return 0, err
	}

	log.Trace("LogStore.LastIndex", "index", i)
	return i, nil
}

// GetLog gets a log entry at a given index.
func (s *LogStore) GetLog(index uint64, l *raft.Log) error {
	err := s.inmem.GetLog(index, l)
	if err != nil {
		log.WithError(err).Trace("LogStore.GetLog failed")
		return err
	}

	log.Trace("LogStore.GetLog", "index", l.Index, "term", l.Term, "type", l.Type, "data_len", len(l.Data), "appendedAt", l.AppendedAt)
	return nil
}

// StoreLog stores a log entry.
func (s *LogStore) StoreLog(l *raft.Log) error {
	log.Trace("LogStore.StoreLog", "index", l.Index, "term", l.Term, "type", l.Type, "data_len", len(l.Data), "appendedAt", l.AppendedAt)
	return s.inmem.StoreLog(l)
}

// StoreLogs stores multiple log entries.
func (s *LogStore) StoreLogs(logs []*raft.Log) error {
	log.Trace("LogStore.StoreLogs", "len", len(logs))
	return s.inmem.StoreLogs(logs)
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *LogStore) DeleteRange(min, max uint64) error {
	log.Trace("LogStore.DeleteRange", "min", min, "max", max)
	return s.inmem.DeleteRange(min, max)
}

func (s *StableStore) Set(key []byte, val []byte) error {
	log.Trace("StableStore.Set", "key", string(key), "val", string(val))
	return s.inmem.Set(key, val)
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *StableStore) Get(key []byte) ([]byte, error) {
	val, err := s.inmem.Get(key)
	if err != nil {
		log.Trace("StableStore.Get failed", "key", string(key))
		return nil, err
	}

	log.Trace("StableStore.Get", "key", string(key), "val", string(val))
	return val, nil
}

func (s *StableStore) SetUint64(key []byte, val uint64) error {
	log.Trace("StableStore.SetUint64", "key", string(key), "val", val)
	return s.inmem.SetUint64(key, val)
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *StableStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.inmem.GetUint64(key)
	if err != nil {
		log.Trace("StableStore.GetUint64 failed", "key", string(key))
		return 0, err
	}

	log.Trace("StableStore.GetUint64", "key", string(key), "val", val)
	return val, nil
}

// Create is used to begin a snapshot at a given index and term, and with
// the given committed configuration. The version parameter controls
// which snapshot version to create.
func (s *SnapshotStore) Create(version raft.SnapshotVersion, index, term uint64,
	cfg raft.Configuration, cfgIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	log.Trace("SnapshotStore.Create", "index", index, "term", term, "cfg_index", cfgIndex)
	return s.inmem.Create(version, index, term, cfg, cfgIndex, trans)
}

// List is used to list the available snapshots in the store.
// It should return then in descending order, with the highest index first.
func (s *SnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	log.Trace("SnapshotStore.List")
	return s.inmem.List()
}

// Open takes a snapshot ID and provides a ReadCloser. Once close is
// called it is assumed the snapshot is no longer needed.
func (s *SnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	log.Trace("SnapshotStore.Open", "id", id)
	return s.inmem.Open(id)
}
