package storage

import (
	"bytes"
	"slices"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ Store = (*store)(nil)
)

// Store defines all possilbe way to interact with the Raft group and its backing FSM storage.
// Read operations are executed directly on the FSM backing storage.
// Write operations wait for the result/error from the FSM.
type Store interface {
	Get(keyspace uint64, key []byte) ([]byte, bool)

	Set(*Set) error
	Delete(*Delete) error
}

type store struct {
	raft *multiraft.Raft
	fsm  *FSM
}

func NewStore(raft *multiraft.Raft, fsm *FSM) Store {
	return &store{
		raft: raft,
		fsm:  fsm,
	}
}

func (s *store) Get(keyspace uint64, key []byte) ([]byte, bool) {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()

	ks, ok := s.fsm.keyspaces[keyspace]
	if !ok {
		return nil, false
	}

	i, found := slices.BinarySearchFunc(ks.Items, key, func(kv *Keyspace_KV, key []byte) int {
		return bytes.Compare(kv.Key, key)
	})
	if !found {
		return nil, false
	}

	return ks.Items[i].Value, true
}

func (s *store) Set(cmd *Set) error {
	return apply(s.raft, cmd)
}

func (s *store) Delete(cmd *Delete) error {
	return apply(s.raft, cmd)
}

func applyR[R any](raft *multiraft.Raft, payload payload) (R, error) {
	var zero R
	cmd, err := newCommand(payload)
	if err != nil {
		return zero, err
	}

	data, err := utils.MarshalProto(cmd)
	if err != nil {
		return zero, err
	}

	result, err := raft.Apply(data)
	if err != nil {
		return zero, err
	}

	if t, ok := result.(R); !ok {
		return zero, errors.Errorf("bad result type from apply: expected %T, got %T", zero, result)
	} else {
		return t, nil
	}
}

func apply(raft *multiraft.Raft, payload payload) error {
	result, err := applyR[any](raft, payload)
	if err != nil {
		return err
	} else if result != nil {
		return errors.Errorf("unexpected non-nil response %T from apply", result)
	}
	return nil
}
