package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
)

// Read-only transactions involving multiple partitions are executed in two phases:
//   - a first prepare/negotiate phase where the partitions are queried to discover
//     the latest possible commit timestamp computed as the min timstamp returned
//     from all partitions. This commit timestamp ensures that all reads are performed
//     from a globally-consistent and causality-preserving snapshot.
//   - a second commit/fetch phase where results are fetched using the commit timestamp
//     from the first phase and is equivalent to a snapshot read at that timestamp.
type processorReadOnly struct {
	client data.ServiceClient
}

func (p *processorReadOnly) Process(ctx context.Context, txn *Txn) (*TxnResult, error) {
	status, err := p.prepare(ctx, txn)
	if err != nil {
		return nil, errors.Wrapf(err, "read-only txn=%s failed to prepare.", txn.id)
	}

	commitTimestamp, commit := p.decide(status)
	if !commit {
		return p.aggregateResults(txn, commit, commitTimestamp, status), nil
	}

	status, err = p.commit(ctx, commitTimestamp, txn)
	if err != nil {
		return nil, errors.Wrapf(err, "read-only txn=%s commit failed.", txn.id)
	}

	return p.aggregateResults(txn, commit, commitTimestamp, status), nil
}

func (p *processorReadOnly) prepare(ctx context.Context, txn *Txn) (statusMap, error) {
	type prepareResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	resultCh := make(chan prepareResult, 1)
	invokePrepare := func(pid uint32) {
		req := &data.PrepareRequest{
			ParticipantPid: pid,
			ReadOnly:       true,
			Txn: &data.Txn{
				Id:      txn.id,
				Actions: txn.participantActions[pid],
			}}

		status, err := p.client.Prepare(ctx, req)
		resultCh <- prepareResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	for pid := range txn.participantActions {
		go invokePrepare(pid)
	}

	status := statusMap{}
	errs := make([]error, 0, txn.ParticipantCount())

	for range txn.ParticipantCount() {
		r := <-resultCh
		status[r.pid] = r.status
		if r.err != nil {
			errs = append(errs, r.err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *processorReadOnly) decide(status statusMap) (uint64, bool) {
	commitTimestamp := uint64(0)

	for _, s := range status {
		if s.State == data.TxnState_Prepared {
			// commit hlc timestamp is min of participant timestamps
			commitTimestamp = min(commitTimestamp, s.Timestamp)
			continue
		}

		return 0, false
	}

	return commitTimestamp, true
}

func (p *processorReadOnly) commit(ctx context.Context, commitTimestamp uint64, txn *Txn) (statusMap, error) {
	type commitResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	resultCh := make(chan commitResult, 1)
	invokeCommit := func(pid uint32) {
		req := &data.CommitRequest{
			ParticipantPid:  pid,
			Id:              txn.id,
			CommitTimestamp: commitTimestamp,
			FetchResults:    true,
		}

		status, err := p.client.Commit(ctx, req)
		resultCh <- commitResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	for pid := range txn.participantActions {
		go invokeCommit(pid)
	}

	status := statusMap{}
	var errs []error

	for range txn.ParticipantCount() {
		r := <-resultCh

		if r.err != nil {
			errs = append(errs, errors.Wrapf(r.err, "read-only txn=%s commit failed at participant %d.", txn.id, r.pid))
		} else if r.status.State != data.TxnState_Committed {
			errs = append(errs, errors.Errorf("read-only txn=%s commit failed with state %s at participant %d.", txn.id, r.status.State, r.pid))
		} else {
			status[r.pid] = r.status
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *processorReadOnly) aggregateResults(txn *Txn, commit bool, commitTimestamp uint64, status statusMap) *TxnResult {
	actionStatus := map[uint32]*data.ActionStatus{}
	for _, s := range status {
		utils.AppendMap(actionStatus, s.ActionStatus)
	}

	result := &TxnResult{
		Id:           txn.id,
		Success:      commit,
		ActionStatus: actionStatus,
	}

	if commit {
		result.Timestamp = commitTimestamp
	}

	return result
}
