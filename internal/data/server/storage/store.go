package storage

import (
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ Store = (*store)(nil)
)

// Store exposes read-only operations executed directly on the FSM backing
// storage, bypassing the Raft algorithm, which are not guaranteed to return
// the latest commited data.
type Store interface {
	Get(key []byte) ([]byte, bool)
	Set(*Set) error
	Del(*Delete) error
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

func (s *store) Get(key []byte) ([]byte, bool) {
	s.fsm.lock.Lock()
	defer s.fsm.lock.Unlock()

	value, ok := s.fsm.items[string(key)]
	return value, ok
}

func (s *store) Set(cmd *Set) error {
	return apply(s.raft, cmd)
}

func (s *store) Del(cmd *Delete) error {
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
