package storage

import (
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
	HasData() bool
	AppliedIndex() uint64
	Get(keyspace uint64, key []byte) ([]byte, bool)

	WriteTxnBatch(*TxnBatch) error
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

func (s *store) HasData() bool {
	return s.AppliedIndex() != 0
}

func (s *store) AppliedIndex() uint64 {
	s.fsm.lock.RLock()
	index := s.fsm.index
	s.fsm.lock.RUnlock()
	return index
}

func (s *store) Get(keyspace uint64, key []byte) ([]byte, bool) {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()

	return nil, true
}

func (s *store) WriteTxnBatch(cmd *TxnBatch) error {
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
