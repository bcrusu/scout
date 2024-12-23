package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
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
	client client.DataClient
}

func (p *processor2PC) Process(ctx context.Context, t *Txn) (*TxnResult, error) {
	status, err := p.prepare(ctx, t)
	if err != nil {
		return nil, errors.Wrapf(err, "2pc txn %s failed to prepare.", t.id)
	} else if len(status) == 1 {
		// status contains only the failed response from the principal
		principalStatus := status[t.id.PrincipalPid]

		return &TxnResult{
			Id:           t.id.ToProto(),
			Timestamp:    principalStatus.Timestamp,
			Success:      false,
			ActionStatus: principalStatus.ActionStatus,
		}, nil
	}

	decision := p.decide(t.id, status)

	if !decision.Commit {
		// Second phase abort could happen in an async fashion, in the background,
		// after the method returns, but this could interfere with scenarios where
		// the client retries the transaction faster than held locks are released.
		//
		// Implements the "presumed abort" optimization which does not store the
		// abort decision. If the current server fails during abort, the principal
		// partition watchdog will trigger and perform the cleanup for us.
		p.abort(ctx, t, status)

		return p.aggregateResults(decision, status), nil
	}

	if s, err := p.client.StoreDecision(ctx, decision); err != nil {
		p.abort(ctx, t, status)
		return nil, errors.Wrapf(err, "2pc txn %s failed to store decision.", t.id)
	} else if s.State != data.TxnStatus_Decided {
		// The principal partition watchdog was faster than us and timedout the txn.
		// Nothing to do here as the second phase abort is already underway...
		return nil, errors.Wrapf(err, "2pc txn %s failed with state %s.", t.id, s.State)
	}

	status, err = p.commit(ctx, decision, t)
	if err != nil {
		return nil, errors.Wrapf(err, "2pc txn %s failed to commit.", t.id)
	}

	return p.aggregateResults(decision, status), nil
}

func (p *processor2PC) prepare(ctx context.Context, t *Txn) (statusMap, error) {
	type prepareResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	txnId := t.id.ToProto()
	resultCh := make(chan prepareResult, 1)
	invokePrepare := func(pid uint32) {
		req := &data.PrepareRequest{
			ParticipantPid: pid,
			ReadOnly:       false,
			Txn: &data.Txn{
				Id:      txnId,
				Actions: t.participantActions[pid],
			}}

		if pid == t.id.PrincipalPid {
			req.Txn.ParticipantPids = utils.MakeKeySlice(t.participantActions)
		}

		status, err := p.client.Prepare(ctx, req)
		resultCh <- prepareResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	status := statusMap{}
	errs := make([]error, 0, t.ParticipantCount()-1)

	handleResult := func(r prepareResult) {
		status[r.pid] = r.status
		if r.err != nil {
			errs = append(errs, errors.Wrapf(r.err, "participant %d failed.", r.pid))
		}
	}

	// First prepare the principal partition which notifies its watchdog.
	invokePrepare(t.id.PrincipalPid)
	handleResult(<-resultCh)

	// Stop early if principal failed
	if len(errs) > 0 {
		return nil, errors.Wrap(errs[0], "principal failed")
	} else if s := status[t.id.PrincipalPid]; s.State != data.TxnStatus_Prepared {
		return status, nil
	}

	// Then prepare the rest of participants
	for pid := range t.participantActions {
		if pid != t.id.PrincipalPid {
			go invokePrepare(pid)
		}
	}

	for range t.ParticipantCount() - 1 {
		handleResult(<-resultCh)
	}

	// Abort if any returned error
	if len(errs) > 0 {
		p.abort(ctx, t, status)
		logErrors(ctx, "2pc txn prepare failed.", t.id, errs)
		return nil, errors.Errorf("participants failed")
	}

	return status, nil
}

func (p *processor2PC) decide(id id, status statusMap) *data.Decision {
	txnId := id.ToProto()
	commitTimestamp := uint64(0)

	for _, s := range status {
		if s.State == data.TxnStatus_Prepared {
			// commit hlc timestamp is max of participant timestamps
			commitTimestamp = max(commitTimestamp, s.Timestamp)
			continue
		}

		return &data.Decision{Id: txnId, Commit: false}
	}

	return &data.Decision{
		Id:              txnId,
		Commit:          true,
		CommitTimestamp: commitTimestamp,
	}
}

func (p *processor2PC) commit(ctx context.Context, decision *data.Decision, t *Txn) (statusMap, error) {
	type commitResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	txnId := t.id.ToProto()
	resultCh := make(chan commitResult, 1)
	invokeCommit := func(pid uint32) {
		req := &data.CommitRequest{
			ParticipantPid:  pid,
			Id:              txnId,
			CommitTimestamp: decision.CommitTimestamp,
			FetchResults:    true,
			ReadOnly:        false,
		}

		status, err := p.client.Commit(ctx, req)
		resultCh <- commitResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	// principal is commited last as it acts as watchdog
	for pid := range t.participantActions {
		if pid != decision.Id.PrincipalPid {
			go invokeCommit(pid)
		}
	}

	status := statusMap{}
	var errs []error

	handleResult := func(r commitResult) {
		if r.err != nil {
			errs = append(errs, errors.Wrapf(r.err, "participant %d failed.", r.pid))
		} else if r.status.State != data.TxnStatus_Committed {
			errs = append(errs, errors.Errorf("participant %d failed with state %s.", r.pid, r.status.State))
		} else {
			status[r.pid] = r.status
		}
	}

	for range t.ParticipantCount() - 1 {
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
	if len(errs) > 0 {
		logErrors(ctx, "2pc txn commit failed.", t.id, errs)
		return nil, errors.Errorf("participants failed")
	}

	invokeCommit(decision.Id.PrincipalPid)
	handleResult(<-resultCh)

	// Similar to the above comment, we are missing the last piece of the puzzle
	// to form a complete result for the client.
	if len(errs) > 0 {
		logErrors(ctx, "2pc txn commit failed at principal.", t.id, errs)
		return nil, errors.Errorf("principal failed")
	}

	return status, nil
}

func (p *processor2PC) abort(ctx context.Context, t *Txn, status statusMap) {
	txnId := t.id.ToProto()
	resultCh := make(chan error, 1)
	invokeAbort := func(pid uint32) {
		req := &data.AbortRequest{
			ParticipantPid: pid,
			Id:             txnId,
		}

		status, err := p.client.Abort(ctx, req)

		switch {
		case err != nil:
			if !errors.Is(err, errors.NotFound) {
				resultCh <- err
			}
			resultCh <- nil
		case !status.State.IsFinal():
			resultCh <- errors.Errorf("participant %d failed with state %s.", pid, status.State)
		default:
			resultCh <- nil
		}
	}

	inFlight := 0
	for pid := range t.participantActions {
		if pid == t.id.PrincipalPid {
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
		invokeAbort(t.id.PrincipalPid)
		handleResult(<-resultCh)
	}

	if len(errs) > 0 {
		logErrors(ctx, "2pc txn abort failed.", t.id, errs)
	}
}

func (p *processor2PC) aggregateResults(decision *data.Decision, status statusMap) *TxnResult {
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
