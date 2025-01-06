package txn

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/tracing"
	"github.com/bcrusu/scout/internal/utils"
	"golang.org/x/time/rate"
)

var (
	_ utils.Lifecycle = (*watchdog2PC)(nil)
)

// The two-phase commit protocol is initiated and coordinated by the originating API server
// in a best-effort basis with the transaction principal partition acting as watchdog and
// secondary coordinator.
//
// The principal partition holds the overall txn state and the watchdog timers. It is:
//   - first to be prepared and has veto vote to end the process early
//   - last to be commited, and
//   - last to be aborted, singaling the end of the process.
//
// The watchdog checks periodically if the prepared txn has not transitioned to the next
// expected phase and takes action. It is essentially racing the API server:
//   - in phase1 to take the txn abort decision, and
//   - in phase2 to complete the txn commit/abort operation.
//
// In phase1, the watchdog tries to mark the txn as Timedout and only if the transition
// is successful proceeds to abort, but if the FSM returns FailedPrecondition error it signals
// that the API server commit decision was faster and the abort is averted. After all the
// participants aborted successfully the locks for the principal/current partition are released
// to mark the end of the process.
//
// In phase2, the watchdog sends the commit/abort request to all txm participants without
// querying their status first as all the 2pc txn operations are implemented to be idempotent,
// returning the previous state on duplicate requests.
type watchdog2PC struct {
	config     config.Transactions
	pid        uint32
	manager    *Manager
	writer     *writer
	client     data.ServiceClient
	log        logging.Logger
	requestCh  chan running
	breaker    *rate.Limiter
	cancelFunc context.CancelFunc
}

type dogQueue = *utils.Queue[running]
type dogResolve func(context.Context, running)

func newWatchdog2PC(pid uint32, writer *writer, manager *Manager, client data.ServiceClient) *watchdog2PC {
	config := config.Get().Transactions

	return &watchdog2PC{
		config:    config,
		pid:       pid,
		manager:   manager,
		writer:    writer,
		client:    client,
		log:       logging.New("2pc_watchdog").With("pid", pid),
		requestCh: make(chan running, 1),
		breaker:   utils.NewRateLimiter(config.RetryBreakerLimit, time.Second),
	}
}

func (w *watchdog2PC) Start(ctx context.Context) error {
	w.cancelFunc = utils.RunAsync(ctx, w.mainLoop)
	return nil
}

func (w *watchdog2PC) Stop() {
	w.cancelFunc()
}

func (w *watchdog2PC) mainLoop(ctx context.Context) {
	tickerPhase1 := time.NewTicker(w.config.Phase1Timeout / 5)
	tickerPhase2 := time.NewTicker(w.config.Phase2Timeout / 5)
	defer tickerPhase1.Stop()
	defer tickerPhase2.Stop()

	all, prepared, decided := w.loadRunning()

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	doneCh := make(chan bool, 1)
	inFlight := 0

	checkTimedout := func(queue dogQueue, timeout time.Duration, expectedState data.TxnStatus_State, resolve dogResolve) {
		oldest := hlc.FromTime(time.Now().Add(-timeout))
		for {
			if peek, ok := queue.PeekFront(); !ok || peek.Timestamp > oldest {
				break
			} else if txn, _ := queue.PopFront(); all[txn.Id] != expectedState {
				continue
			} else {
				inFlight++
				delete(all, txn.Id)

				go func() {
					resolve(cancelCtx, txn)
					doneCh <- true
				}()
			}
		}
	}

	for {
		select {
		case status := <-w.requestCh:
			if _, ok := all[status.Id]; !ok {
				continue
			}

			switch status.State {
			case data.TxnStatus_Prepared:
				all[status.Id] = status.State
				prepared.PushBack(status)
			case data.TxnStatus_Decided:
				all[status.Id] = status.State
				decided.PushBack(status)
			case data.TxnStatus_Committed, data.TxnStatus_Aborted:
				delete(all, status.Id) // checkTimedout above will later clear from queue
			}
		case <-tickerPhase1.C:
			checkTimedout(prepared, w.config.Phase1Timeout, data.TxnStatus_Prepared, w.abort)
		case <-tickerPhase2.C:
			checkTimedout(decided, w.config.Phase2Timeout, data.TxnStatus_Decided, w.commit)
		case <-doneCh:
			inFlight--
		case <-ctx.Done():
			cancelFunc()

			for ; inFlight > 0; inFlight-- {
				<-doneCh
			}

			return
		}
	}
}

// UpdateStatus takes the latest applied status returned by the FSM, and:
//   - the prepared != nil only for Prepare call.
//   - the decision != nil only for StoreDecision call.
func (w *watchdog2PC) UpdateStatus(status *data.TxnStatus, prepared *data.Txn, decision *data.Decision) {
	if status == nil || status.Id.PrincipalPid != w.pid {
		return
	}

	s := running{
		Id:        newId(status.Id),
		Timestamp: status.Timestamp,
		State:     status.State,
		Decision:  decision,
	}

	if prepared != nil {
		s.ParticipantPids = prepared.ParticipantPids
	}

	w.requestCh <- s
}

func (w *watchdog2PC) loadRunning() (map[id]data.TxnStatus_State, dogQueue, dogQueue) {
	trunning := w.manager.getRunning()

	all := map[id]data.TxnStatus_State{}
	prepared := make([]running, 0, len(trunning))
	decided := make([]running, 0, len(trunning))

	for _, p := range trunning {
		switch p.State {
		case data.TxnStatus_Prepared:
			prepared = append(prepared, p)
		case data.TxnStatus_Decided:
			decided = append(decided, p)
		case data.TxnStatus_Timedout:
			// A running txn in Timedout state happens when the previous Raft partition
			// leader has started the abort procedure (i.e. marked the txn as timedout),
			// but lost leadership before the abort operation completed which did not
			// release the locks. Add it to prepared queue and the ticker will invoke
			// the abort goroutine on the first tick.
			prepared = append(prepared, p)
		default:
			w.log.Warn("Unexpected running txn.", "id", p.Id, "state", p.State)
			continue
		}

		all[p.Id] = p.State
	}

	cmpFn := func(a, b running) int { return int(a.Timestamp - b.Timestamp) }
	slices.SortFunc(prepared, cmpFn)
	slices.SortFunc(decided, cmpFn)

	return all, utils.NewQueue(prepared...), utils.NewQueue(decided...)
}

func (w *watchdog2PC) abort(ctx context.Context, txn running) {
	if success, err := w.markTimedout(ctx, txn, false); err != nil || !success {
		return
	}

	resultCh := make(chan error, 1)
	invokeAbort := func(pid uint32) {
		req := &data.AbortRequest{
			ParticipantPid: pid,
			Id:             txn.Id.ToProto(),
		}

		resultCh <- utils.RetryForeverE(ctx, &w.config.RetryPolicy.Backoff, w.withBreaker(func() error {
			status, err := w.client.Abort(ctx, req)

			switch {
			case err != nil:
				if errors.Is(err, errors.NotFound) {
					return nil
				}

				w.log.WithContext(ctx).WithError(err).Error("Failed to abort. Retrying...", "id", txn.Id, "participant", pid)
				return err
			case !status.State.IsFinal():
				return errors.Errorf("participant %d failed with state %s.", pid, status.State)
			default:
				return nil
			}
		}))
	}

	for _, pid := range txn.ParticipantPids {
		// principal is aborted last
		if pid != txn.Id.PrincipalPid {
			go invokeAbort(pid)
		}
	}

	var errs []error
	for range len(txn.ParticipantPids) - 1 {
		if err := <-resultCh; err != nil {
			errs = append(errs, err)
		}
	}

	// reaching here with errors means that either the ctx was canceled
	// or the circuit breaker triggered.
	if err := errors.Join(errs...); err != nil {
		w.log.WithContext(ctx).WithError(err).Errorf("2pc txn %s failed to abort.", txn.Id)
		return
	}

	// lastly, release the locks for us which marks the end of the process.
	if success, err := w.markTimedout(ctx, txn, true); err != nil || !success {
		w.log.WithContext(ctx).WithError(err).Errorf("2pc txn %s principal failed to release locks.", txn.Id)
	}
}

func (w *watchdog2PC) markTimedout(ctx context.Context, txn running, releaseLocks bool) (bool, error) {
	cmd := &data.MarkTimedout{
		Id:           txn.Id.ToProto(),
		ReleaseLocks: releaseLocks,
		Trace:        tracing.GetTraceID(ctx),
	}

	success := false
	err := utils.RetryForeverE(ctx, &w.config.RetryPolicy.Backoff, w.withBreaker(func() error {
		_, err := w.writer.Apply(cmd)

		switch {
		case err != nil:
			if errors.IsAny(err, errors.NotFound, errors.FailedPrecondition) {
				// the API server was faster
				success = false
				return nil
			}

			w.log.WithContext(ctx).WithError(err).Error("Failed to mark timedout. Retrying...", "id", txn.Id)
			return err
		default:
			success = true
			return nil
		}
	}))

	return success, err
}

func (w *watchdog2PC) commit(ctx context.Context, txn running) {
	if !txn.Decision.Commit {
		// The 2pc implementation does not store abort decisions with the "presumed abort"
		// optimization. This execution path should be unreachable.
		panic(fmt.Sprintf("2pc txn %s unexpected abort decision during commit.", txn.Id))
	}

	resultCh := make(chan error, 1)
	invokeCommit := func(pid uint32) {
		req := &data.CommitRequest{
			ParticipantPid:  pid,
			Id:              txn.Id.ToProto(),
			CommitTimestamp: txn.Decision.CommitTimestamp,
			FetchResults:    false,
			ReadOnly:        false,
		}

		resultCh <- utils.RetryForeverE(ctx, &w.config.RetryPolicy.Backoff, w.withBreaker(func() error {
			status, err := w.client.Commit(ctx, req)

			switch {
			case err != nil:
				if errors.Is(err, errors.NotFound) {
					return nil
				}

				w.log.WithContext(ctx).WithError(err).Error("Failed to commit. Retrying...", "id", txn.Id, "participant", pid)
				return err
			case status.State != data.TxnStatus_Committed:
				return errors.Errorf("participant %d failed with state %s.", pid, status.State)
			default:
				return nil
			}
		}))
	}

	for _, pid := range txn.ParticipantPids {
		// principal is commited last
		if pid != txn.Id.PrincipalPid {
			go invokeCommit(pid)
		}
	}

	var errs []error
	for range len(txn.ParticipantPids) - 1 {
		if err := <-resultCh; err != nil {
			errs = append(errs, err)
		}
	}

	// reaching here with errors means that either the ctx was canceled
	// or the circuit breaker triggered.
	if err := errors.Join(errs...); err != nil {
		w.log.WithContext(ctx).WithError(err).Errorf("2pc txn %s failed to commit.", txn.Id)
		return
	}

	invokeCommit(txn.Id.PrincipalPid)
	if err := <-resultCh; err != nil {
		w.log.WithContext(ctx).WithError(err).Errorf("2pc txn %s principal failed to commit.", txn.Id)
	}
}

func (w *watchdog2PC) withBreaker(work func() error) func() error {
	return func() error {
		err := work()
		if err != nil && !w.breaker.Allow() {
			utils.GracefulShutdown("2pc watchdog failed too many times")
		}
		return err
	}
}
