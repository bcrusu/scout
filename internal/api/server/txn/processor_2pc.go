package txn

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

// The two-phase commit protocol is initiated and coordinated by the originating API server
// in a best-effort basis with the transaction principal partition acting as watchdog and
// secondary coordinator.
//
// The principal partition holds the overall txn state and the watchdog timers. It is:
//   - first to be prepared and has veto vote to end the process early
//   - last to be commited, and
//   - last to be aborted, singaling the end of the process.
type processor2PC struct {
	id     identity.Identity
	client data.ServiceClient
}

type statusMap map[uint32]*data.TxnStatus

func (p *processor2PC) Process(ctx context.Context, txn *Txn) (*TxnResult, error) {
	status, err := p.prepare(ctx, txn)
	if err != nil {
		return nil, errors.Wrapf(err, "2pc txn=%s failed to prepare.", txn.id)
	} else if len(status) == 1 {
		// status contains only the failed response from the principal
		principalStatus := status[txn.id.PrincipalPid]

		return &TxnResult{
			Id:           txn.id,
			Timestamp:    principalStatus.Timestamp,
			Success:      false,
			ActionStatus: principalStatus.ActionStatus,
		}, nil
	}

	decision := p.decide(txn.id, status)

	if !decision.Commit {
		// Second phase abort could happen in an async fashion, in the background,
		// after the method returns, but this could interfere with scenarios where
		// the client retries the transaction faster than held locks are released.
		//
		// Implements the "presumed abort" optimization which does not store the
		// abort decision. If the current server fails during abort, the principal
		// partition watchdog will trigger and perform the cleanup for us.
		p.abort(ctx, txn, status)

		return p.aggregateResults(decision, status), nil
	}

	if s, err := p.client.StoreDecision(ctx, decision); err != nil {
		p.abort(ctx, txn, status)
		return nil, errors.Wrapf(err, "2pc txn=%s failed to store decision.", txn.id)
	} else if s.State != data.TxnState_Decided {
		// The principal partition watchdog was faster than us and timedout the txn.
		// Nothing to do here as the second phase abort is already underway...
		return nil, errors.Wrapf(err, "2pc txn=%s failed with state %s.", txn.id, s.State)
	}

	status, err = p.commit(ctx, decision, txn)
	if err != nil {
		return nil, errors.Wrapf(err, "2pc txn=%s commit failed.", txn.id)
	}

	return p.aggregateResults(decision, status), nil
}

func (p *processor2PC) prepare(ctx context.Context, txn *Txn) (statusMap, error) {
	type prepareResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	resultCh := make(chan prepareResult, 1)
	invokePrepare := func(pid uint32) {
		req := &data.PrepareRequest{
			ParticipantPid: pid,
			Txn: &data.Txn{
				Id:      txn.id,
				Actions: txn.participantActions[pid],
			}}

		if pid == txn.id.PrincipalPid {
			req.Txn.ParticipantPids = utils.MakeKeySlice(txn.participantActions)
		}

		status, err := p.client.Prepare(ctx, req)
		resultCh <- prepareResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	status := statusMap{}
	errs := make([]error, 0, len(txn.participantActions)-1)

	handleResult := func(r prepareResult) {
		status[r.pid] = r.status
		errs = append(errs, r.err)
	}

	// First prepare the principal partition which notifies its watchdog.
	invokePrepare(txn.id.PrincipalPid)
	handleResult(<-resultCh)

	// Stop early if principal failed
	if len(errs) > 0 {
		return nil, errs[0]
	} else if s := status[txn.id.PrincipalPid]; s.State != data.TxnState_Prepared {
		return status, nil
	}

	// Then prepare the rest of participants
	for pid := range txn.participantActions {
		if pid != txn.id.PrincipalPid {
			go invokePrepare(pid)
		}
	}

	for range len(txn.participantActions) - 1 {
		handleResult(<-resultCh)
	}

	// Abort if any returned error
	if err := errors.Join(errs...); err != nil {
		p.abort(ctx, txn, status)
		return nil, err
	}

	return status, nil
}

func (p *processor2PC) decide(id *data.TxnId, status statusMap) *data.TxnDecision {
	commitTimestamp := uint64(0)

	for _, s := range status {
		if s.State == data.TxnState_Prepared {
			// commit hlc timestamp is max of participant timestamps
			commitTimestamp = max(commitTimestamp, s.Timestamp)
			continue
		}

		return &data.TxnDecision{Id: id, Commit: false}
	}

	return &data.TxnDecision{
		Id:              id,
		Commit:          true,
		CommitTimestamp: commitTimestamp,
	}
}

func (p *processor2PC) commit(ctx context.Context, decision *data.TxnDecision, txn *Txn) (statusMap, error) {
	type commitResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	resultCh := make(chan commitResult, 1)
	invokeCommit := func(pid uint32) {
		req := &data.CommitRequest{
			ParticipantPid: pid,
			Id:             txn.id,
			Timestamp:      decision.CommitTimestamp,
			FetchResults:   true,
		}

		status, err := p.client.Commit(ctx, req)
		resultCh <- commitResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	// principal is commited last as it acts as watchdog
	for pid := range txn.participantActions {
		if pid != decision.Id.PrincipalPid {
			go invokeCommit(pid)
		}
	}

	status := statusMap{}
	var errs []error

	handleResult := func(r commitResult) {
		if r.err != nil {
			errs = append(errs, errors.Wrapf(r.err, "2pc txn=%s commit failed at participant %d.", txn.id, r.pid))
		} else if r.status.State != data.TxnState_Committed {
			errs = append(errs, errors.Errorf("2pc txn=%s commit failed with state %s at participant %d.", txn.id, r.status.State, r.pid))
		} else {
			status[r.pid] = r.status
		}
	}

	for range len(txn.participantActions) - 1 {
		handleResult(<-resultCh)
	}

	// Even though the eventual txn commited state is known from the decision,
	// we are missing the action results from some partitions which prevents
	// a complete and consistent result at this moment.
	// Client will need to query txn status to discover the result.
	//
	// If any participant failed to commit, return early without calling commit
	// on principal partition. This allows the watchdog to trigger and restart
	// the commit phase from scratch.
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	invokeCommit(decision.Id.PrincipalPid)
	handleResult(<-resultCh)

	// Similar to the above comment, we are missing the last piece of the puzzle
	// to form a complete result for the client.
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *processor2PC) abort(ctx context.Context, txn *Txn, status statusMap) {
	resultCh := make(chan error, 1)
	invokeAbort := func(pid uint32) {
		req := &data.AbortRequest{
			ParticipantPid: pid,
			Id:             txn.id,
		}

		status, err := p.client.Abort(ctx, req)

		switch {
		case err != nil:
			if !errors.Is(err, errors.NotFound) {
				resultCh <- err
			}
			resultCh <- nil
		case !status.State.IsFinal():
			resultCh <- errors.Errorf("2pc txn=%s abort failed with state %s at participant %d.", txn.id, status.State, pid)
		default:
			resultCh <- nil
		}
	}

	inFlight := 0
	for pid := range txn.participantActions {
		if pid == txn.id.PrincipalPid {
			continue // principal is aborted last
		} else if s := status[pid]; s != nil && s.State.IsFinal() {
			continue
		}

		// send abort also to participants that returned error; the request
		// could have been processsed while the response got lost.
		go invokeAbort(pid)
		inFlight++
	}

	var errs []error
	handleResult := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	for ; inFlight > 0; inFlight-- {
		handleResult(<-resultCh)
	}

	// On error, does not cancel the principal so its watchdog can retry the abort.
	if len(errs) == 0 {
		invokeAbort(txn.id.PrincipalPid)
		handleResult(<-resultCh)
	}

	if err := errors.Join(errs...); err != nil {
		logging.WithError(err).Errorf(ctx, "2pc txn=%s abort failed.", txn.id)
	}
}

func (p *processor2PC) aggregateResults(decision *data.TxnDecision, status statusMap) *TxnResult {
	actionStatus := map[uint32]*data.ActionStatus{}
	for _, s := range status {
		utils.AppendMap(actionStatus, s.ActionStatus)
	}

	result := &TxnResult{
		Id:           decision.Id,
		Success:      decision.Commit,
		ActionStatus: actionStatus,
	}

	if decision.Commit {
		result.Timestamp = decision.CommitTimestamp
	}

	return result
}
