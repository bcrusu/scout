package txn

import (
	"context"
	"fmt"
	"time"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/errors"
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
	mainLoop, cancelFunc := utils.WithCancelAndWait(s.mainLoop)

	s.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (s *batcher) Stop() {
	s.cancelFunc()
}

func (s *batcher) Apply(payload any) (*Status, error) {
	cmd := applyCmd{
		payload:  payload,
		resultCh: make(chan BatchResult, 1),
	}

	s.applyCh <- cmd
	r := <-cmd.resultCh
	return r.Status, r.Error
}

func (s *batcher) mainLoop(ctx context.Context) {
	for {
		var timer *time.Timer

		batch := &Batch{}
		waiting := &batchWaiting{}
		batchSize := 0

		nextBatch := func() {
			asyncCh, err := s.raftStore.ApplyBatch(batch)
			if err != nil {
				s.sendBatchErr(waiting, err)
			} else {
				go s.waitBatchResult(waiting, asyncCh)
			}

			batch = &Batch{}
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
			case *Autocommit:
				batch.Autocommit = append(batch.Autocommit, x)
				waiting.Autocommit = append(waiting.Autocommit, cmd.resultCh)
			case *Prepare:
				batch.Prepare = append(batch.Prepare, x)
				waiting.Prepare = append(waiting.Prepare, cmd.resultCh)
			case *Commit:
				batch.Commit = append(batch.Commit, x)
				waiting.Commit = append(waiting.Commit, cmd.resultCh)
			case *Abort:
				batch.Abort = append(batch.Abort, x)
				waiting.Abort = append(waiting.Abort, cmd.resultCh)
			case *StoreDecision:
				batch.StoreDecision = append(batch.StoreDecision, x)
				waiting.StoreDecision = append(waiting.StoreDecision, cmd.resultCh)
			case *MarkTimedout:
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
