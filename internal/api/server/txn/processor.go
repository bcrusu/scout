package txn

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/client"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/hlc"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   utils.Lifecycle = (*Processor)(nil)
	log                 = logging.WithComponent("txn")
)

type Config struct {
	BatchMaxSize    int           `yaml:"batchMaxSize" default:"16" validate:"min:1,max:128"`
	BatchMaxActions int           `yaml:"batchMaxActions" default:"128" validate:"min:1,max:1024"`
	BatchMaxDelay   time.Duration `yaml:"batchMaxDelay" default:"50ms" validate:"min:10ms,max:5s"`
}

type Processor struct {
	id          identity.Identity
	config      Config
	client      client.DataClient
	partitioner *partitioner
	batcher     *batcher
	cancelFunc  context.CancelFunc
}

func NewProcessor(id identity.Identity, config Config, client client.DataClient) *Processor {
	return &Processor{
		id:      id,
		config:  config,
		client:  client,
		batcher: newBatcher(config),
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

func (p *Processor) Stop() {
}

// TODO
func (p *Processor) Get(ctx context.Context, key []byte) ([]byte, time.Time, error) {
	return nil, time.Time{}, nil
}

func (p *Processor) GetAt(ctx context.Context, key []byte, at time.Time) ([]byte, time.Time, error) {
	return nil, time.Time{}, nil
}

func (p *Processor) ExecuteSingle(ctx context.Context, routingKey []byte, action *data.Action) (*data.TxnStatus, error) {
	partitionId := p.partitioner.getPartition(routingKey)
	txn := &data.Txn{
		Id:      p.newTxnId(),
		Actions: []*data.Action{action},
	}

	return p.batcher.executeSingle(ctx, partitionId, txn)
}

func (p *Processor) ExecuteMulti(ctx context.Context, multi *Multi) (*data.TxnBatchStatus, error) {
	if len(multi.byPartition) == 0 {
		return nil, errors.InvalidRequest
	}

	return p.batcher.executeMulti(ctx, multi)
}

func (p *Processor) Multi() *Multi {
	return &Multi{
		id:          p.newTxnId(),
		partitioner: p.partitioner,
		byPartition: map[uint32]*data.Txn{},
	}
}

func (p *Processor) newTxnId() *data.TxnId {
	return &data.TxnId{
		ServerId:  p.id.ServerID,
		Timestamp: hlc.Now(),
	}
}
