package client

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
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
	txn.TxnServiceClient
	utils.Lifecycle
}

type dataClient struct {
	opts      *options
	conn      *rpc.Conn
	client    data.ServiceClient
	txnClient txn.TxnServiceClient
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
	c.txnClient = txn.NewTxnServiceClient(c.conn)

	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *dataClient) Stop() {
	utils.LifecycleStop(logC.NoContext(), c.conn)
}

func (c *dataClient) Autocommit(ctx context.Context, req *txn.AutocommitRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	ctx = withRouting(ctx, routing{
		partitionID:  req.PartitionId,
		snapshotRead: req.IsSnapshotRead(),
	})

	return c.txnClient.Autocommit(ctx, req, opts...)
}

func (c *dataClient) Prepare(ctx context.Context, req *txn.PrepareRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	ctx = withRouting(ctx, routing{
		partitionID:  req.ParticipantPid,
		snapshotRead: false,
	})

	return c.txnClient.Prepare(ctx, req, opts...)
}

func (c *dataClient) Commit(ctx context.Context, req *txn.CommitRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	ctx = withRouting(ctx, routing{
		partitionID:  req.ParticipantPid,
		snapshotRead: false,
	})

	return c.txnClient.Commit(ctx, req, opts...)
}

func (c *dataClient) Abort(ctx context.Context, req *txn.AbortRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	ctx = withRouting(ctx, routing{
		partitionID:  req.ParticipantPid,
		snapshotRead: false,
	})

	return c.txnClient.Abort(ctx, req, opts...)
}

func (c *dataClient) StoreDecision(ctx context.Context, dec *txn.Decision, opts ...grpc.CallOption) (*txn.Status, error) {
	ctx = withRouting(ctx, routing{
		partitionID:  dec.Id.PrincipalPid,
		snapshotRead: false,
	})

	return c.txnClient.StoreDecision(ctx, dec, opts...)
}

func (c *dataClient) StreamPartition(ctx context.Context, req *data.StreamRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[data.StreamResponse], error) {
	ctx = withRouting(ctx, routing{
		partitionID:  req.PartitionId,
		snapshotRead: true,
	})

	return c.client.StreamPartition(ctx, req, opts...)
}
