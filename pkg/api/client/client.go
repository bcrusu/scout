package client

import (
	"context"

	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/bcrusu/graph/pkg/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
)

var (
	_    Client = (*client)(nil)
	logC        = logging.WithComponent("api_client")
)

func init() {
	balancer.Register(&balancerBuilder{})
}

type Client interface {
	api.KeyValueClient
	api.GraphClient
	utils.Lifecycle
}

type client struct {
	api.KeyValueClient
	api.GraphClient
	opts *options
	conn *rpc.Conn
}

func New(opts ...Option) Client {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	return &client{
		opts: o,
	}
}

func (c *client) Start(ctx context.Context) error {
	if err := c.opts.target.Validate(); err != nil {
		return err
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{}))
	c.conn = rpc.NewConn(c.opts.target.String(), dialOpts...)
	c.KeyValueClient = api.NewKeyValueClient(c.conn)
	c.GraphClient = api.NewGraphClient(c.conn)

	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *client) Stop() {
	utils.LifecycleStop(logC.NoContext(), c.conn)
}
