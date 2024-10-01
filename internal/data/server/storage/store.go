package storage

import (
	"slices"

	"github.com/bcrusu/graph/internal/data"
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

	GetTxnRunning() []TxnRunning
	TxnAutocommit(*TxnAutocommit) (*data.TxnStatus, error)
	TxnPrepare(*TxnPrepare) (*data.TxnStatus, error)
	TxnCommit(*TxnCommit) (*data.TxnStatus, error)
	TxnAbort(*TxnAbort) (*data.TxnStatus, error)
	StoreTxnDecision(*StoreTxnDecision) (*data.TxnStatus, error)
	MarkTxnTimedout(cmd *MarkTxnTimedout) (*data.TxnStatus, error)
	TxnBatch(*TxnBatch) (*TxnBatchResult, error)
}

type TxnRunning struct {
	Id              TxnId
	Timestamp       uint64
	State           data.TxnState
	ParticipantPids []uint32
	Decision        *data.TxnDecision
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

func (s *store) GetTxnRunning() []TxnRunning {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()

	result := make([]TxnRunning, 0, len(s.fsm.txnProcessor.prepared))

	for id, p := range s.fsm.txnProcessor.prepared {
		status := s.fsm.txnProcessor.status[id]

		result = append(result, TxnRunning{
			Id:              id,
			Timestamp:       status.Timestamp,
			State:           status.State,
			ParticipantPids: slices.Clone(p.Txn.ParticipantPids),
			Decision:        utils.CloneProto(s.fsm.txnProcessor.decisions[id]),
		})
	}

	return result
}

func (s *store) TxnAutocommit(cmd *TxnAutocommit) (*data.TxnStatus, error) {
	return applyR[*data.TxnStatus](s.raft, cmd)
}

func (s *store) TxnPrepare(cmd *TxnPrepare) (*data.TxnStatus, error) {
	return applyR[*data.TxnStatus](s.raft, cmd)
}

func (s *store) TxnCommit(cmd *TxnCommit) (*data.TxnStatus, error) {
	return applyR[*data.TxnStatus](s.raft, cmd)
}

func (s *store) TxnAbort(cmd *TxnAbort) (*data.TxnStatus, error) {
	return applyR[*data.TxnStatus](s.raft, cmd)
}

func (s *store) StoreTxnDecision(cmd *StoreTxnDecision) (*data.TxnStatus, error) {
	return applyR[*data.TxnStatus](s.raft, cmd)
}

func (s *store) MarkTxnTimedout(cmd *MarkTxnTimedout) (*data.TxnStatus, error) {
	return applyR[*data.TxnStatus](s.raft, cmd)
}

func (s *store) TxnBatch(cmd *TxnBatch) (*TxnBatchResult, error) {
	return applyR[*TxnBatchResult](s.raft, cmd)
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
