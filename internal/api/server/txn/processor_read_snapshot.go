package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
)

// Read-only snapshot transactions are executed in a scatter-gather fashion with all
// involved partitions queried using the same snapshot read timestamp.
type processorReadSnapshot struct {
	client data.ServiceClient
}

func (p *processorReadSnapshot) Process(ctx context.Context, txn *Txn) (*TxnResult, error) {
	status, err := p.autocommit(ctx, txn)
	if err != nil {
		return nil, errors.Wrapf(err, "snapshot read txn=%s failed.", txn.id)
	}

	return p.aggregateResults(txn, status), nil
}

func (p *processorReadSnapshot) autocommit(ctx context.Context, txn *Txn) (statusMap, error) {
	type prepareResult struct {
		pid    uint32
		status *data.TxnStatus
		err    error
	}

	resultCh := make(chan prepareResult, 1)
	invokeAutocommit := func(pid uint32) {
		req := &data.AutocommitRequest{
			PartitionId:   pid,
			ReadTimestamp: txn.readTimestamp,
			Txn: &data.Txn{
				Id:      txn.id,
				Actions: txn.participantActions[pid],
			}}

		status, err := p.client.Autocommit(ctx, req)
		resultCh <- prepareResult{
			pid:    pid,
			status: status,
			err:    err,
		}
	}

	for pid := range txn.participantActions {
		go invokeAutocommit(pid)
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

func (p *processorReadSnapshot) aggregateResults(txn *Txn, status statusMap) *TxnResult {
	actionStatus := map[uint32]*data.ActionStatus{}
	for _, s := range status {
		utils.AppendMap(actionStatus, s.ActionStatus)
	}

	return &TxnResult{
		Id:           txn.id,
		Timestamp:    txn.readTimestamp,
		Success:      true,
		ActionStatus: actionStatus,
	}
}
