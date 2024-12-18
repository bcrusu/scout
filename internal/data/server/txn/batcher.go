package txn

import (
	"context"
	"fmt"
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ utils.Lifecycle = (*batcher)(nil)
)

type batcher struct {
	config     config.Transactions
	raftStore  RaftStore
	applyCh    chan applyCmd
	cancelFunc context.CancelFunc
}

type applyCmd struct {
	payload  any
	resultCh chan BatchResult
}

type batchWaiting struct {
	Autocommit    []chan BatchResult
	Prepare       []chan BatchResult
	Commit        []chan BatchResult
	Abort         []chan BatchResult
	StoreDecision []chan BatchResult
	MarkTimedout  []chan BatchResult
}

func newBatcher(raftStore RaftStore) *batcher {
	return &batcher{
		config:    config.Get().Transactions,
		raftStore: raftStore,
		applyCh:   make(chan applyCmd),
	}
}

func (s *batcher) Start(ctx context.Context) error {
	s.cancelFunc = utils.RunAsync(ctx, s.mainLoop)
	return nil
}

func (s *batcher) Stop() {
	s.cancelFunc()
}

func (s *batcher) Apply(payload any) (*data.TxnStatus, error) {
	cmd := applyCmd{
		payload:  payload,
		resultCh: make(chan BatchResult, 1),
	}

	s.applyCh <- cmd
	r := <-cmd.resultCh
	return r.Status, r.Error
}

func (s *batcher) mainLoop(ctx context.Context) {
	var timer *time.Timer

	batch := &data.TxnBatch{}
	waiting := &batchWaiting{}
	batchSize := 0

	nextBatch := func() {
		s.prepareBatch(batch)
		asyncCh := s.raftStore.ApplyBatch(batch)
		go s.waitBatchResult(waiting, asyncCh)

		batch = &data.TxnBatch{}
		waiting = &batchWaiting{}
		batchSize = 0
		timer.Stop()
	}

	for {
		select {
		case <-utils.GetTimerChan(timer):
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

func (s *batcher) waitBatchResult(waiting *batchWaiting, resultCh <-chan multiraft.AsyncResult) {
	asyncResult := <-resultCh
	if asyncResult.Error != nil {
		s.sendBatchErr(waiting, asyncResult.Error)
		return
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

func (s *batcher) sendBatchErr(waiting *batchWaiting, err error) {
	result := BatchResult{Error: err}

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
//   - Abort/StoreDecision/MarkTimedout timestamps have only informative role
//     and could have been set elsewhere.
func (s *batcher) prepareBatch(batch *data.TxnBatch) {
	for _, x := range batch.Autocommit {
		x.Timestamp = hlc.Now()
	}

	for _, x := range batch.Prepare {
		x.Timestamp = hlc.Now()
	}

	for _, x := range batch.Abort {
		x.Timestamp = hlc.Now()
	}

	for _, x := range batch.StoreDecision {
		x.Timestamp = hlc.Now()
	}

	for _, x := range batch.MarkTimedout {
		x.Timestamp = hlc.Now()
	}
}
