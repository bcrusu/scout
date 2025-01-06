package txn

import (
	"context"
	"fmt"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ utils.Lifecycle = (*writer)(nil)
)

type writer struct {
	config     config.Transactions
	raftStore  RaftStore
	applyCh    chan applyCmd
	log        logging.Logger
	cancelFunc context.CancelFunc
}

type applyCmd struct {
	payload  any
	resultCh chan BatchResult
}

type updateTimestamp struct {
	Timestamp uint64
}

type batchWaiting struct {
	Autocommit      []chan BatchResult
	Prepare         []chan BatchResult
	Commit          []chan BatchResult
	Abort           []chan BatchResult
	StoreDecision   []chan BatchResult
	MarkTimedout    []chan BatchResult
	UpdateTimestamp []chan BatchResult
}

func newWriter(raftStore RaftStore, log logging.Logger) *writer {
	return &writer{
		config:    config.Get().Transactions,
		raftStore: raftStore,
		log:       log,
		applyCh:   make(chan applyCmd),
	}
}

func (s *writer) Start(ctx context.Context) error {
	s.cancelFunc = utils.RunAsync(ctx, s.mainLoop)
	return nil
}

func (s *writer) Stop() {
	s.cancelFunc()
}

// Apply sends the command payload to Raft.
func (s *writer) Apply(payload any) (*data.TxnStatus, error) {
	cmd := applyCmd{
		payload:  payload,
		resultCh: make(chan BatchResult, 1),
	}

	s.applyCh <- cmd
	r := <-cmd.resultCh
	return r.Status, r.Error
}

// UpdateTimestamp bumps the max timestamp to the provided value.
// It assumes that the value is inside the max allowed time offset and
// already validated by a hlc.Update() call by the uspream caller.
// It is used to solve the similar problem described in Spanner, Section
// 4.2.4 "Refinements", where the 'safe' timestamp cannot advance in the
// absence of writes, and thus reads cannot happen at timestamps above the
// last written timestamp.
func (s *writer) UpdateTimestamp(timestamp uint64) error {
	cmd := applyCmd{
		payload:  updateTimestamp{Timestamp: timestamp},
		resultCh: make(chan BatchResult, 1),
	}

	s.applyCh <- cmd
	r := <-cmd.resultCh
	return r.Error
}

func (s *writer) mainLoop(ctx context.Context) {
	timer := utils.NewTimer(s.config.MaxBatchDelay)
	defer timer.Stop()

	batch := &data.TxnBatch{}
	waiting := &batchWaiting{}
	batchSize := 0

	nextBatch := func() {
		s.prepareBatch(batch)
		resultCh := s.raftStore.ApplyBatch(batch)
		go s.waitBatchResult(waiting, resultCh)

		batch = &data.TxnBatch{}
		waiting = &batchWaiting{}
		batchSize = 0
		timer.Stop()
	}

	for {
		select {
		case <-timer.C:
			nextBatch()
		case cmd := <-s.applyCh:
			switch x := cmd.payload.(type) {
			case *data.Autocommit:
				batch.Autocommit = append(batch.Autocommit, x)
				waiting.Autocommit = append(waiting.Autocommit, cmd.resultCh)
			case *data.Prepare:
				batch.Prepare = append(batch.Prepare, x)
				waiting.Prepare = append(waiting.Prepare, cmd.resultCh)
			case *data.Commit:
				batch.Commit = append(batch.Commit, x)
				waiting.Commit = append(waiting.Commit, cmd.resultCh)
			case *data.Abort:
				batch.Abort = append(batch.Abort, x)
				waiting.Abort = append(waiting.Abort, cmd.resultCh)
			case *data.StoreDecision:
				batch.StoreDecision = append(batch.StoreDecision, x)
				waiting.StoreDecision = append(waiting.StoreDecision, cmd.resultCh)
			case *data.MarkTimedout:
				batch.MarkTimedout = append(batch.MarkTimedout, x)
				waiting.MarkTimedout = append(waiting.MarkTimedout, cmd.resultCh)
			case updateTimestamp:
				batch.MaxTimestamp = max(batch.MaxTimestamp, x.Timestamp)
				waiting.UpdateTimestamp = append(waiting.UpdateTimestamp, cmd.resultCh)
			default:
				panic(fmt.Sprintf("unhandled command payload type %T", cmd.payload))
			}

			batchSize++
			if batchSize == 1 {
				timer.Reset(s.config.MaxBatchDelay)
			} else if batchSize == s.config.MaxBatchSize {
				nextBatch()
			}
		case <-ctx.Done():
			if batchSize != 0 {
				s.sendBatchErr(waiting, errors.Unavailable)
			}

			return
		}
	}
}

func (s *writer) waitBatchResult(waiting *batchWaiting, resultCh <-chan multiraft.AsyncResult) {
	asyncResult := <-resultCh
	if asyncResult.Error != nil {
		s.sendBatchErr(waiting, asyncResult.Error)
		return
	}

	for _, waiting := range waiting.UpdateTimestamp {
		waiting <- BatchResult{Error: nil}
	}

	results := asyncResult.Result.(*BatchResults)

	for i, result := range results.Autocommit {
		waiting.Autocommit[i] <- result
	}
	for i, result := range results.Prepare {
		waiting.Prepare[i] <- result
	}
	for i, result := range results.Commit {
		waiting.Commit[i] <- result
	}
	for i, result := range results.Abort {
		waiting.Abort[i] <- result
	}
	for i, result := range results.StoreDecision {
		waiting.StoreDecision[i] <- result
	}
	for i, result := range results.MarkTimedout {
		waiting.MarkTimedout[i] <- result
	}
}

func (s *writer) sendBatchErr(waiting *batchWaiting, err error) {
	result := BatchResult{Error: err}

	for _, ch := range waiting.UpdateTimestamp {
		ch <- result
	}
	for _, ch := range waiting.Autocommit {
		ch <- result
	}
	for _, ch := range waiting.Prepare {
		ch <- result
	}
	for _, ch := range waiting.Commit {
		ch <- result
	}
	for _, ch := range waiting.Abort {
		ch <- result
	}
	for _, ch := range waiting.StoreDecision {
		ch <- result
	}
	for _, ch := range waiting.MarkTimedout {
		ch <- result
	}
}

// Sets only the HLC timestamps for new, but any other future optimizations
// related to txn ordering, write dedup/merging, etc, could happen at this stage.
//   - ordering is important with all Autocommit timestamps less than Prepare
//     timestamps which efectively makes Autocommit writes come before in the
//     MVCC than any future Commit writes corresponding to the Prepare.
//   - Commit timestamp is not set here as it was already set by the upstream
//     caller as determined by 2PC txn participants.
func (s *writer) prepareBatch(batch *data.TxnBatch) {
	maxTS := uint64(0)

	hlcNow := func() uint64 {
		maxTS = hlc.Now()
		return maxTS
	}

	for _, x := range batch.Autocommit {
		x.Timestamp = hlcNow()
	}

	for _, x := range batch.Prepare {
		x.Timestamp = hlcNow()
	}

	for _, x := range batch.Abort {
		x.Timestamp = hlcNow()
	}

	for _, x := range batch.StoreDecision {
		x.Timestamp = hlcNow()
	}

	for _, x := range batch.MarkTimedout {
		x.Timestamp = hlcNow()
	}

	for _, x := range batch.Commit {
		maxTS = max(maxTS, x.Timestamp)
	}

	batch.MaxTimestamp = max(batch.MaxTimestamp, maxTS)
}
