package storage

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ Store = (*store)(nil)
)

// Store defines all possilbe way to interact with the Raft group and its backing FSM storage.
// Read operations are executed directly on the FSM backing storage.
// Write operations wait for the result/error from the FSM.
type Store interface {
	utils.Lifecycle

	HasData() bool
	AppliedIndex() uint64
	Get(keyspace uint64, key []byte) ([]byte, bool)

	GetRunning() []TxnRunning
	Autocommit(*data.Txn) (*data.TxnStatus, error)
	Prepare(*data.Txn) (*data.TxnStatus, error)
	Commit(id *data.TxnId, commitTimestamp uint64) (*data.TxnStatus, error)
	Abort(*data.TxnId) (*data.TxnStatus, error)
	StoreDecision(*data.TxnDecision) (*data.TxnStatus, error)
	MarkTimedout(id *data.TxnId, releaseLocks bool) (*data.TxnStatus, error)
}

type TxnRunning struct {
	Id              TxnId
	Timestamp       uint64
	State           data.TxnState
	ParticipantPids []uint32
	Decision        *data.TxnDecision
}

type store struct {
	config     config.Transactions
	raft       *multiraft.Raft
	fsm        *FSM
	applyCh    chan applyCmd
	cancelFunc context.CancelFunc
}

type applyCmd struct {
	payload  any
	resultCh chan TxnStatus
}

type batchWaiting struct {
	Autocommit    []chan TxnStatus
	Prepare       []chan TxnStatus
	Commit        []chan TxnStatus
	Abort         []chan TxnStatus
	StoreDecision []chan TxnStatus
	MarkTimedout  []chan TxnStatus
}

func NewStore(raft *multiraft.Raft, fsm *FSM) Store {
	return &store{
		config:  config.Get().Transactions,
		raft:    raft,
		fsm:     fsm,
		applyCh: make(chan applyCmd),
	}
}

func (s *store) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(s.mainLoop)

	s.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (s *store) Stop() {
	s.cancelFunc()
}

func (s *store) mainLoop(ctx context.Context) {
	for {
		var timer *time.Timer

		batch := &TxnBatch{}
		waiting := &batchWaiting{}
		batchSize := 0

		nextBatch := func() {
			asyncCh, err := applyAsync(s.raft, batch)
			if err != nil {
				s.sendBatchErr(waiting, err)
			} else {
				go s.waitBatchResult(waiting, asyncCh)
			}

			batch = &TxnBatch{}
			waiting = &batchWaiting{}
			batchSize = 0
			timer.Stop()
		}

		var timerCh <-chan time.Time
		if timer != nil {
			timerCh = timer.C
		}

		select {
		case <-timerCh:
			nextBatch()
		case cmd := <-s.applyCh:
			switch x := cmd.payload.(type) {
			case *TxnAutocommit:
				x.Timestamp = hlc.Now()
				batch.Autocommit = append(batch.Autocommit, x)
				waiting.Autocommit = append(waiting.Autocommit, cmd.resultCh)
			case *TxnPrepare:
				x.Timestamp = hlc.Now()
				batch.Prepare = append(batch.Prepare, x)
				waiting.Prepare = append(waiting.Prepare, cmd.resultCh)
			case *TxnCommit:
				hlc.Update(x.Timestamp) // the commit timestamp is decided by txn participants
				batch.Commit = append(batch.Commit, x)
				waiting.Commit = append(waiting.Commit, cmd.resultCh)
			case *TxnAbort:
				x.Timestamp = hlc.Now()
				batch.Abort = append(batch.Abort, x)
				waiting.Abort = append(waiting.Abort, cmd.resultCh)
			case *StoreTxnDecision:
				x.Timestamp = hlc.Now()
				batch.StoreDecision = append(batch.StoreDecision, x)
				waiting.StoreDecision = append(waiting.StoreDecision, cmd.resultCh)
			case *MarkTxnTimedout:
				x.Timestamp = hlc.Now()
				batch.MarkTimedout = append(batch.MarkTimedout, x)
				waiting.MarkTimedout = append(waiting.MarkTimedout, cmd.resultCh)
			default:
				panic(fmt.Sprintf("unhandled command payload type %T", cmd.payload))
			}

			batchSize++
			if batchSize == 1 {
				if timer == nil {
					timer = time.NewTimer(s.config.MaxBatchDelay)
				} else {
					timer.Reset(s.config.MaxBatchDelay)
				}
			} else if batchSize == s.config.MaxBatchSize {
				nextBatch()
			}
		case <-ctx.Done():
			if batchSize != 0 {
				timer.Stop()
				s.sendBatchErr(waiting, errors.Unavailable)
			}

			return
		}
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

	//TODO

	return nil, true
}

func (s *store) GetRunning() []TxnRunning {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()

	result := make([]TxnRunning, 0, len(s.fsm.txnProcessor.prepared))

	for id := range s.fsm.txnProcessor.prepared {
		status := s.fsm.txnProcessor.status[id]

		result = append(result, TxnRunning{
			Id:              id,
			Timestamp:       status.Timestamp,
			State:           status.State,
			ParticipantPids: slices.Clone(status.ParticipantPids),
			Decision:        utils.CloneProto(s.fsm.txnProcessor.decisions[id]),
		})
	}

	return result
}

func (s *store) Autocommit(txn *data.Txn) (*data.TxnStatus, error) {
	return s.apply(&TxnAutocommit{Txn: txn})
}

func (s *store) Prepare(txn *data.Txn) (*data.TxnStatus, error) {
	return s.apply(&TxnPrepare{Txn: txn})
}

func (s *store) Commit(id *data.TxnId, commitTimestamp uint64) (*data.TxnStatus, error) {
	return s.apply(&TxnCommit{Id: id, Timestamp: commitTimestamp})
}

func (s *store) Abort(id *data.TxnId) (*data.TxnStatus, error) {
	return s.apply(&TxnAbort{Id: id})
}

func (s *store) StoreDecision(dec *data.TxnDecision) (*data.TxnStatus, error) {
	return s.apply(&StoreTxnDecision{Decision: dec})
}

func (s *store) MarkTimedout(id *data.TxnId, releaseLocks bool) (*data.TxnStatus, error) {
	return s.apply(&MarkTxnTimedout{Id: id, ReleaseLocks: releaseLocks})
}

func (s *store) apply(payload any) (*data.TxnStatus, error) {
	cmd := applyCmd{
		payload:  payload,
		resultCh: make(chan TxnStatus, 1),
	}

	s.applyCh <- cmd
	r := <-cmd.resultCh
	return r.Status, r.Error
}

func (s *store) waitBatchResult(waiting *batchWaiting, resultCh <-chan multiraft.AsyncResult) {
	asyncResult := <-resultCh
	if asyncResult.Error != nil {
		s.sendBatchErr(waiting, asyncResult.Error)
		return
	}

	result := asyncResult.Result.(*TxnBatchResult)

	for i, status := range result.Autocommit {
		waiting.Autocommit[i] <- status
	}
	for i, status := range result.Prepare {
		waiting.Prepare[i] <- status
	}
	for i, status := range result.Commit {
		waiting.Commit[i] <- status
	}
	for i, status := range result.Abort {
		waiting.Abort[i] <- status
	}
	for i, status := range result.StoreDecision {
		waiting.StoreDecision[i] <- status
	}
	for i, status := range result.MarkTimedout {
		waiting.MarkTimedout[i] <- status
	}
}

func (s *store) sendBatchErr(waiting *batchWaiting, err error) {
	status := TxnStatus{Error: err}

	for _, ch := range waiting.Autocommit {
		ch <- status
	}
	for _, ch := range waiting.Prepare {
		ch <- status
	}
	for _, ch := range waiting.Commit {
		ch <- status
	}
	for _, ch := range waiting.Abort {
		ch <- status
	}
	for _, ch := range waiting.StoreDecision {
		ch <- status
	}
	for _, ch := range waiting.MarkTimedout {
		ch <- status
	}
}

func applyR[R any](raft *multiraft.Raft, payload cmdPayload) (R, error) {
	var zero R
	cmd := newCommand(payload)

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

func applyAsync(raft *multiraft.Raft, payload cmdPayload) (<-chan multiraft.AsyncResult, error) {
	cmd := newCommand(payload)

	data, err := utils.MarshalProto(cmd)
	if err != nil {
		return nil, err
	}

	return raft.ApplyAsync(data), nil
}
