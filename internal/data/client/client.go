package client

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
)

var (
	_    DataClient = (*dataClient)(nil)
	logC            = logging.WithComponent("data_client")
)

func init() {
	balancer.Register(&balancerBuilder{})
}

// DataClient is the Data service client.
type DataClient interface {
	data.ServiceClient
	utils.Lifecycle
}

type dataClient struct {
	opts   *options
	conn   *rpc.Conn
	client data.ServiceClient
}

func New(opts ...Option) DataClient {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return &dataClient{
		opts: o,
	}
}

func (c *dataClient) Start(ctx context.Context) error {
	if c.conn != nil {
		return nil
	} else if c.opts.clusterName == "" {
		return errors.Error("missing cluster name")
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{}))
	c.conn = rpc.NewConn(dummyTarget, c.opts.clusterName, dialOpts...)
	c.client = data.NewServiceClient(c.conn)

	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *dataClient) Stop() {
	utils.LifecycleStop(logC.NoContext(), c.conn)
}

func (c *dataClient) ExecuteTxnBatch(ctx context.Context, batch *data.TxnBatch, opts ...grpc.CallOption) (*data.TxnBatchStatus, error) {
	ctx = withRouting(ctx, routing{
		partitionID: batch.PartitionId,
		replicaRead: false,
	})

	return c.client.ExecuteTxnBatch(ctx, batch, opts...)
}
