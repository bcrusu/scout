package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/client"
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
		return nil, errors.Wrapf(err, "snapshot read txn %s failed.", t.id)
	}

	return p.aggregateResults(t, status), nil
}

func (p *processorReadSnapshot) autocommit(ctx context.Context, t *Txn) (statusMap, error) {
	type prepareResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	txnId := t.id.ToProto()
	resultCh := make(chan prepareResult, 1)
	invokeAutocommit := func(pid uint32) {
		req := &data.AutocommitRequest{
			PartitionId:   pid,
			ReadTimestamp: t.readTimestamp,
			Txn: &data.Txn{
				Id:      txnId,
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

	if len(errs) > 0 {
		logErrors(ctx, "snapshot read txn commit failed.", t.id, errs)
		return nil, errors.Errorf("participants failed")
	}

	return status, nil
}

func (p *processorReadSnapshot) aggregateResults(t *Txn, status statusMap) *TxnResult {
	actionStatus := map[uint32]*data.ActionStatus{}
	for _, s := range status {
		utils.AppendMap(actionStatus, s.ActionStatus)
	}

	return &TxnResult{
		Id:           t.id.ToProto(),
		Timestamp:    t.readTimestamp,
		Success:      true,
		ActionStatus: actionStatus,
	}
}
