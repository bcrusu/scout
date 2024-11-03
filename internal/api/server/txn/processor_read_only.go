package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/client"
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
	client client.DataClient
}

func (p *processorReadOnly) Process(ctx context.Context, t *Txn) (*TxnResult, error) {
	status, err := p.prepare(ctx, t)
	if err != nil {
		return nil, errors.Wrapf(err, "read-only txn=%s failed to prepare.", t.id)
	}

	commitTimestamp, commit := p.decide(status)
	if !commit {
		return p.aggregateResults(t, commit, commitTimestamp, status), nil
	}

	status, err = p.commit(ctx, commitTimestamp, t)
	if err != nil {
		return nil, errors.Wrapf(err, "read-only txn=%s commit failed.", t.id)
	}

	return p.aggregateResults(t, commit, commitTimestamp, status), nil
}

func (p *processorReadOnly) prepare(ctx context.Context, t *Txn) (statusMap, error) {
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
				Id:      t.id,
				Actions: t.participantActions[pid],
			}}

		status, err := p.client.Prepare(ctx, req)
		resultCh <- prepareResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	for pid := range t.participantActions {
		go invokePrepare(pid)
	}

	status := statusMap{}
	errs := make([]error, 0, t.ParticipantCount())

	for range t.ParticipantCount() {
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
		if s.State == data.TxnStatus_Prepared {
			// commit hlc timestamp is min of participant timestamps
			commitTimestamp = min(commitTimestamp, s.Timestamp)
			continue
		}

		return 0, false
	}

	return commitTimestamp, true
}

func (p *processorReadOnly) commit(ctx context.Context, commitTimestamp uint64, t *Txn) (statusMap, error) {
	type commitResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	resultCh := make(chan commitResult, 1)
	invokeCommit := func(pid uint32) {
		req := &data.CommitRequest{
			ParticipantPid:  pid,
			Id:              t.id,
			CommitTimestamp: commitTimestamp,
			FetchResults:    true,
			ReadOnly:        true,
		}

		status, err := p.client.Commit(ctx, req)
		resultCh <- commitResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	for pid := range t.participantActions {
		go invokeCommit(pid)
	}

	status := statusMap{}
	var errs []error

	for range t.ParticipantCount() {
		r := <-resultCh

		if r.err != nil {
			errs = append(errs, errors.Wrapf(r.err, "read-only txn=%s commit failed at participant %d.", t.id, r.pid))
		} else if r.status.State != data.TxnStatus_Committed {
			errs = append(errs, errors.Errorf("read-only txn=%s commit failed with state %s at participant %d.", t.id, r.status.State, r.pid))
		} else {
			status[r.pid] = r.status
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *processorReadOnly) aggregateResults(t *Txn, commit bool, commitTimestamp uint64, status statusMap) *TxnResult {
	actionStatus := map[uint32]*data.ActionStatus{}
	for _, s := range status {
		utils.AppendMap(actionStatus, s.ActionStatus)
	}

	result := &TxnResult{
		Id:           t.id,
		Success:      commit,
		ActionStatus: actionStatus,
	}

	if commit {
		result.Timestamp = commitTimestamp
	}

	return result
}
