package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/api/server/config"
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_   utils.Lifecycle = (*Processor)(nil)
	log                 = logging.WithComponent("txn")
)

type Processor struct {
	identity              identity.Identity
	partitioner           *partitioner
	client                client.DataClient
	processor2pc          *processor2PC
	processorReadOnly     *processorReadOnly
	processorReadSnapshot *processorReadSnapshot
}

type statusMap map[uint32]*txn.Status

func NewProcessor(id identity.Identity, client client.DataClient) *Processor {
	client = &clientRetrier{
		DataClient: client,
		policy:     config.Get().Transactions.RetryPolicy,
	}

	return &Processor{
		identity:              id,
		client:                client,
		processor2pc:          &processor2PC{client: client},
		processorReadOnly:     &processorReadOnly{client: client},
		processorReadSnapshot: &processorReadSnapshot{client: client},
	}
}

func (p *Processor) Start(ctx context.Context) error {
	sub := eventbus.Subscribe[*control.ApiServerConfig]()
	defer sub.Unsubscribe()

	select {
	case cfg := <-sub.Items():
		// wait for config before moving on
		p.partitioner = newPartitioner(cfg.PartitionCount)
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (p *Processor) Stop() {}

func (p *Processor) Process(ctx context.Context, txn *Txn) (*TxnResult, error) {
	readOnly := txn.IsReadOnly()
	if !readOnly && txn.readTimestamp != 0 {
		return nil, errors.Errorf("snapshot read timestamp invalid for read-write txn=%s.", txn.id)
	}

	switch txn.ParticipantCount() {
	case 0:
		return nil, errors.Error("transaction is empty")
	case 1:
		// transactions involving a single partition can avoid the 2PC dance
		return p.autocommit(ctx, txn)
	default:
		if !readOnly {
			return p.processor2pc.Process(ctx, txn)
		}

		if txn.readTimestamp == 0 {
			return p.processorReadOnly.Process(ctx, txn)
		} else {
			return p.processorReadSnapshot.Process(ctx, txn)
		}
	}
}

func (p *Processor) autocommit(ctx context.Context, t *Txn) (*TxnResult, error) {
	_, actions, _ := utils.GetSingleMapKey(t.participantActions)

	req := &txn.AutocommitRequest{
		PartitionId:   t.id.PrincipalPid,
		ReadTimestamp: t.readTimestamp,
		Txn: &txn.Txn{
			Id:      t.id,
			Actions: actions,
		},
	}

	status, err := p.client.Autocommit(ctx, req)
	if err != nil {
		return nil, err
	}

	return &TxnResult{
		Id:           t.id,
		Timestamp:    status.Timestamp,
		Success:      status.State == txn.Status_Committed,
		ActionStatus: status.ActionStatus,
	}, nil
}

func (p *Processor) New() *Txn {
	return &Txn{
		processor:          p,
		participantActions: map[uint32][]*txn.Action{},
	}
}
