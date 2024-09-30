package client

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/errors"
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
	if c.conn != nil {
		return nil
	} else if c.opts.clusterName == "" {
		return errors.Error("missing cluster name")
	}

	dialOpts := append(c.opts.dialOptions, grpc.WithResolvers(&resolverBuilder{}))
	c.conn = rpc.NewConn(dummyTarget, c.opts.clusterName, dialOpts...)
	c.client = data.NewServiceClient(c.conn)

	return utils.LifecycleStart(ctx, logC, c.conn)
}

func (c *dataClient) Stop() {
	utils.LifecycleStop(logC.NoContext(), c.conn)
}

func (c *dataClient) Autocommit(ctx context.Context, txn *data.Txn, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routing{
		partitionID: txn.Id.PrincipalPid,
		replicaRead: txn.IsReplicaRead(),
	})

	return c.client.Autocommit(ctx, txn, opts...)
}

func (c *dataClient) Prepare(ctx context.Context, req *data.PrepareRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routing{
		partitionID: req.ParticipantPid,
		replicaRead: false,
	})

	return c.client.Prepare(ctx, req, opts...)
}

func (c *dataClient) Commit(ctx context.Context, req *data.CommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routing{
		partitionID: req.ParticipantPid,
		replicaRead: false,
	})

	return c.client.Commit(ctx, req, opts...)
}

func (c *dataClient) Abort(ctx context.Context, req *data.AbortRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routing{
		partitionID: req.ParticipantPid,
		replicaRead: false,
	})

	return c.client.Abort(ctx, req, opts...)
}

func (c *dataClient) StoreDecision(ctx context.Context, dec *data.TxnDecision, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	ctx = withRouting(ctx, routing{
		partitionID: dec.Id.PrincipalPid,
		replicaRead: false,
	})

	return c.client.StoreDecision(ctx, dec, opts...)
}
