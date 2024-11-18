package client

import (
	"context"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/pkg/graph"
	"github.com/bcrusu/scout/pkg/keyvalue"
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
	utils.Lifecycle
	KeyValue() keyvalue.ServiceClient
	Graph() graph.ServiceClient
}

type client struct {
	opts     *options
	conn     *rpc.Conn
	keyValue keyvalue.ServiceClient
	graph    graph.ServiceClient
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
	} else if c.opts.address == "" {
		return errors.Error("missing address")
	}

	config := rpc.ConnConfig{
		Target:      discovery.Servers(c.opts.address).Target(),
		ClusterName: c.opts.clusterName,
		EnableHlc:   false,
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{c.opts}))
	c.conn = rpc.NewConn(config, dialOpts...)
	c.keyValue = keyvalue.NewServiceClient(c.conn)
	c.graph = graph.NewServiceClient(c.conn)

	return c.conn.Start(ctx)
}

func (c *client) Stop() {
	c.conn.Stop()
}

func (c *client) KeyValue() keyvalue.ServiceClient {
	return c.keyValue
}

func (c *client) Graph() graph.ServiceClient {
	return c.graph
}
