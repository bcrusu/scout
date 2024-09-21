package client

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
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
	resolver := &resolverBuilder{}
	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(resolver))

	c.conn = rpc.NewConn(dummyTarget, dialOpts...)
	c.client = data.NewServiceClient(c.conn)

	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *dataClient) Stop() {
	utils.LifecycleStop(logC.NoContext(), c.conn)
}

func (c *dataClient) Set(ctx context.Context, req *data.SetRequest, opts ...grpc.CallOption) (*data.SetResponse, error) {
	ctx = withRouting(ctx, routing{
		partitionID: req.PartitionId,
		isWrite:     true,
	})

	return c.client.Set(ctx, req, opts...)
}

func (c *dataClient) Get(ctx context.Context, req *data.GetRequest, opts ...grpc.CallOption) (*data.GetResponse, error) {
	ctx = withRouting(ctx, routing{
		partitionID: req.PartitionId,
		isWrite:     false,
	})

	return c.client.Get(ctx, req, opts...)
}

func (c *dataClient) Delete(ctx context.Context, req *data.DeleteRequest, opts ...grpc.CallOption) (*data.DeleteResponse, error) {
	ctx = withRouting(ctx, routing{
		partitionID: req.PartitionId,
		isWrite:     true,
	})

	return c.client.Delete(ctx, req, opts...)
}
