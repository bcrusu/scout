package txn

import (
	"context"

	"github.com/bcrusu/graph/internal/api/server/config"
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   utils.Lifecycle = (*Processor)(nil)
	log                 = logging.WithComponent("txn")
)

type Processor struct {
	identity     identity.Identity
	partitioner  *partitioner
	client       data.ServiceClient
	processor2pc *processor2PC
}

func NewProcessor(id identity.Identity, client data.ServiceClient) *Processor {
	client = &clientRetrier{
		policy: config.Get().Transactions.RetryPolicy,
		inner:  client,
	}

	return &Processor{
		identity: id,
		client:   client,
		processor2pc: &processor2PC{
			id:     id,
			client: client,
		},
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
	switch txn.ParticipantCount() {
	case 0:
		return nil, errors.Error("transaction is empty")
	case 1:
		// transactions involving a single partition can avoid the 2PC dance
		return p.autocommit(ctx, txn)
	default:
		return p.processor2pc.Process(ctx, txn)
	}
}

func (p *Processor) Execute(ctx context.Context, routingKey []byte, actions ...*data.Action) (*TxnResult, error) {
	if len(routingKey) == 0 || len(actions) == 0 {
		return nil, errors.InvalidRequest
	}

	txn := p.New().Append(routingKey, actions...)
	return p.autocommit(ctx, txn)
}

func (p *Processor) autocommit(ctx context.Context, txn *Txn) (*TxnResult, error) {
	_, actions, _ := utils.GetSingleMapKey(txn.participantActions)

	req := &data.Txn{
		Id:      txn.id,
		Actions: actions,
	}

	status, err := p.client.Autocommit(ctx, req)
	if err != nil {
		return nil, err
	}

	return &TxnResult{
		Id:           txn.id,
		Timestamp:    status.Timestamp,
		Success:      status.State == data.TxnState_Committed,
		ActionStatus: status.ActionStatus,
	}, nil
}

func (p *Processor) New() *Txn {
	return &Txn{
		processor:          p,
		participantActions: map[uint32][]*data.Action{},
	}
}
