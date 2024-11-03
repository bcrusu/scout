package client

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
)

var (
	_ DataClient = (*dataClient)(nil)
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
	o := newOptions()
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

	config := rpc.ConnConfig{
		Target:      dummyTarget,
		ClusterName: c.opts.clusterName,
		EnableHlc:   true,
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{c.opts}))
	c.conn = rpc.NewConn(config, dialOpts...)
	c.client = data.NewServiceClient(c.conn)

	return c.conn.Start(ctx)
}

func (c *dataClient) Stop() {
	c.conn.Stop()
}

func (c *dataClient) Autocommit(ctx context.Context, req *data.AutocommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routingInfo{
		partitionID:  req.PartitionId,
		snapshotRead: req.IsSnapshotRead(),
	})

	return c.client.Autocommit(ctx, req, opts...)
}

func (c *dataClient) Prepare(ctx context.Context, req *data.PrepareRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routingInfo{
		partitionID:  req.ParticipantPid,
		snapshotRead: false,
	})

	return c.client.Prepare(ctx, req, opts...)
}

func (c *dataClient) Commit(ctx context.Context, req *data.CommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routingInfo{
		partitionID:  req.ParticipantPid,
		snapshotRead: false,
	})

	return c.client.Commit(ctx, req, opts...)
}

func (c *dataClient) Abort(ctx context.Context, req *data.AbortRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routingInfo{
		partitionID:  req.ParticipantPid,
		snapshotRead: false,
	})

	return c.client.Abort(ctx, req, opts...)
}

func (c *dataClient) StoreDecision(ctx context.Context, dec *data.Decision, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routingInfo{
		partitionID:  dec.Id.PrincipalPid,
		snapshotRead: false,
	})

	return c.client.StoreDecision(ctx, dec, opts...)
}

func (c *dataClient) StreamPartition(ctx context.Context, req *data.StreamRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[data.StreamResponse], error) {
	ctx = withRouting(ctx, routingInfo{
		partitionID:  req.PartitionId,
		snapshotRead: true,
	})

	return c.client.StreamPartition(ctx, req, opts...)
}
