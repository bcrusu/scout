package client

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/errors"
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
	control.ServiceClient
	opts *options
	conn *rpc.Conn
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
	} else if c.opts.clusterName == "" {
		return errors.Error("missing cluster name")
	} else if err := c.opts.discovery.Validate(); err != nil {
		return err
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{c.opts.clusterName}))
	c.conn = rpc.NewConn(c.opts.discovery.String(), c.opts.clusterName, dialOpts...)
	c.ServiceClient = control.NewServiceClient(c.conn)

	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *controlClient) Stop() {
	utils.LifecycleStop(logC.NoContext(), c.conn)
}
