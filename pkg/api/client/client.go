package client

import (
	"context"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/pkg/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
)

var (
	_ Client = (*client)(nil)
)

func init() {
	balancer.Register(&balancerBuilder{})
}

type Client interface {
	api.KeyValueServiceClient
	api.GraphServiceClient
	utils.Lifecycle
}

type client struct {
	api.KeyValueServiceClient
	api.GraphServiceClient
	opts *options
	conn *rpc.Conn
}

func New(opts ...Option) Client {
	o := newOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &client{
		opts: o,
	}
}

func (c *client) Start(ctx context.Context) error {
	if c.conn != nil {
		return nil
	} else if c.opts.clusterName == "" {
		return errors.Error("missing cluster name")
	} else if err := c.opts.discovery.Validate(); err != nil {
		return err
	}

	config := rpc.ConnConfig{
		Target:      c.opts.discovery.Target(),
		ClusterName: c.opts.clusterName,
		EnableHlc:   false,
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{c.opts}))
	c.conn = rpc.NewConn(config, dialOpts...)
	c.KeyValueServiceClient = api.NewKeyValueServiceClient(c.conn)
	c.GraphServiceClient = api.NewGraphServiceClient(c.conn)

	return c.conn.Start(ctx)
}

func (c *client) Stop() {
	c.conn.Stop()
}
