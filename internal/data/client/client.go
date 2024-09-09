package client

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/discovery"
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
	data.DataClient
	utils.Lifecycle
}

type Option func(*options)

type dataClient struct {
	conn   *rpc.Conn
	client data.DataClient
}

type options struct {
	target      discovery.Target
	dialOptions []grpc.DialOption
}

func NewClient(opts ...Option) DataClient {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := append(o.dialOptions, grpc.WithResolvers(&resolverBuilder{}))
	conn := rpc.NewConn(o.target.String(), dialOpts...)

	return &dataClient{
		conn:   conn,
		client: data.NewDataClient(conn),
	}
}

func (c *dataClient) Start(ctx context.Context) error {
	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *dataClient) Stop(ctx context.Context) {
	utils.LifecycleStop(ctx, logC, c.conn)
}

func (c *dataClient) Set(ctx context.Context, req *data.SetRequest, opts ...grpc.CallOption) (*data.SetResponse, error) {
	return c.client.Set(ctx, req, opts...)
}

func (c *dataClient) Get(ctx context.Context, req *data.GetRequest, opts ...grpc.CallOption) (*data.GetResponse, error) {
	return c.client.Get(ctx, req, opts...)
}

func (c *dataClient) Delete(ctx context.Context, req *data.DeleteRequest, opts ...grpc.CallOption) (*data.DeleteResponse, error) {
	return c.client.Delete(ctx, req, opts...)
}
