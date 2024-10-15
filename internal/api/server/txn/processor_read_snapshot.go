package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
)

// Read-only snapshot transactions are executed in a scatter-gather fashion with all
// involved partitions queried using the same snapshot read timestamp.
type processorReadSnapshot struct {
	client client.DataClient
}

func (p *processorReadSnapshot) Process(ctx context.Context, t *Txn) (*TxnResult, error) {
	status, err := p.autocommit(ctx, t)
	if err != nil {
		return nil, errors.Wrapf(err, "snapshot read txn=%s failed.", t.id)
	}

	return p.aggregateResults(t, status), nil
}

func (p *processorReadSnapshot) autocommit(ctx context.Context, t *Txn) (statusMap, error) {
	type prepareResult struct {
		pid    uint32
		status *txn.Status
		err    error
	}

	resultCh := make(chan prepareResult, 1)
	invokeAutocommit := func(pid uint32) {
		req := &txn.AutocommitRequest{
			PartitionId:   pid,
			ReadTimestamp: t.readTimestamp,
			Txn: &txn.Txn{
				Id:      t.id,
				Actions: t.participantActions[pid],
			}}

		status, err := p.client.Autocommit(ctx, req)
		resultCh <- prepareResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	for pid := range t.participantActions {
		go invokeAutocommit(pid)
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

func (p *processorReadSnapshot) aggregateResults(t *Txn, status statusMap) *TxnResult {
	actionStatus := map[uint32]*txn.ActionStatus{}
	for _, s := range status {
		utils.AppendMap(actionStatus, s.ActionStatus)
	}

	return &TxnResult{
		Id:           t.id,
		Timestamp:    t.readTimestamp,
		Success:      true,
		ActionStatus: actionStatus,
	}
}
