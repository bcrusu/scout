package follower

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ data.ServiceServer = (*Follower)(nil)
	_ utils.Lifecycle    = (*Follower)(nil)
)

// Follower implements the follower role.
type Follower struct {
	data.UnsafeServiceServer
	log      logging.Logger
	txn      *txn.Service
	streamer *shared.PartitionStreamer
}

func New(pid uint32, db kv.DB, txn *txn.Service) *Follower {
	return &Follower{
		log:      logging.New("follower").With("partition", pid),
		txn:      txn,
		streamer: shared.NewPartitionStreamer(db),
	}
}

func (n *Follower) Start(ctx context.Context) error {
	if err := n.txn.Start(ctx); err != nil {
		return err
	}

	n.log.WithContext(ctx).Debug("Started")
	return nil
}

func (n *Follower) Stop() {
	n.txn.Stop()
	n.log.Debug("Stopped")
}

func (n *Follower) IsLeader() bool {
	return false
}

func (n *Follower) Autocommit(ctx context.Context, req *data.AutocommitRequest) (*data.TxnStatus, error) {
	if !req.IsSnapshotRead() {
		return nil, errors.NotLeader
	}

	return n.txn.Autocommit(ctx, req)
}

func (n *Follower) Prepare(context.Context, *data.PrepareRequest) (*data.TxnStatus, error) {
	return nil, errors.NotLeader
}

func (n *Follower) Commit(context.Context, *data.CommitRequest) (*data.TxnStatus, error) {
	return nil, errors.NotLeader
}

func (n *Follower) Abort(context.Context, *data.AbortRequest) (*data.TxnStatus, error) {
	return nil, errors.NotLeader
}

func (n *Follower) StoreDecision(context.Context, *data.Decision) (*data.TxnStatus, error) {
	return nil, errors.NotLeader
}

func (n *Follower) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	return n.streamer.StreamPartition(req, stream)
}
