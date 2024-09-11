package client

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
)

var (
	_    ControlClient = (*controlClient)(nil)
	logC               = logging.WithComponent("control_client")
)

func init() {
	balancer.Register(&balancerBuilder{})
}

// ControlClient is the Control service client
type ControlClient interface {
	control.ServiceClient
	utils.Lifecycle
}

type controlClient struct {
	conn   *rpc.Conn
	client control.ServiceClient
}

type options struct {
	target      discovery.Target
	dialOptions []grpc.DialOption
}

func NewClient(opts ...Option) ControlClient {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := append(o.dialOptions, grpc.WithResolvers(&resolverBuilder{}))
	conn := rpc.NewConn(o.target.String(), dialOpts...)

	return &controlClient{
		conn:   conn,
		client: control.NewServiceClient(conn),
	}
}

func (c *controlClient) Start(ctx context.Context) error {
	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *controlClient) Stop(ctx context.Context) {
	utils.LifecycleStop(ctx, logC, c.conn)
}

func (c *controlClient) Discover(ctx context.Context, req *control.DiscoverRequest, opts ...grpc.CallOption) (*control.DiscoverResponse, error) {
	return c.client.Discover(ctx, req, opts...)
}

func (c *controlClient) Register(ctx context.Context, req *control.RegisterRequest, opts ...grpc.CallOption) (*control.RegisterResponse, error) {
	return c.client.Register(ctx, req, opts...)
}

func (c *controlClient) NewSession(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[control.SessionIn, control.SessionOut], error) {
	return c.client.NewSession(ctx, opts...)
}
