package client

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
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
	opts   *options
	conn   *rpc.Conn
	client control.ServiceClient
}

func New(opts ...Option) ControlClient {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return &controlClient{
		opts: o,
	}
}

func (c *controlClient) Start(ctx context.Context) error {
	if c.conn != nil {
		return nil
	} else if err := c.opts.target.Validate(); err != nil {
		return err
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{}))
	c.conn = rpc.NewConn(c.opts.target.String(), dialOpts...)
	c.client = control.NewServiceClient(c.conn)

	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *controlClient) Stop() {
	utils.LifecycleStop(logC.NoContext(), c.conn)
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
