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
	_ data.ServiceServer   = (*Follower)(nil)
	_ txn.TxnServiceServer = (*Follower)(nil)
	_ utils.Lifecycle      = (*Follower)(nil)
)

// Follower implements the follower role.
type Follower struct {
	data.UnsafeServiceServer
	txn.UnsafeTxnServiceServer
	log      logging.Logger
	txn      *txn.Service
	streamer *shared.PartitionStreamer
}

func New(pid uint32, db kv.DB, txn *txn.Service) *Follower {
	return &Follower{
		log:      logging.WithComponent("follower").With("partition", pid),
		txn:      txn,
		streamer: shared.NewPartitionStreamer(db),
	}
}

func (n *Follower) Start(ctx context.Context) error {
	if err := n.txn.Start(ctx); err != nil {
		return err
	}

	n.log.Debug(ctx, "Started")
	return nil
}

func (n *Follower) Stop() {
	n.txn.Stop()
	n.log.NoContext().Debug("Stopped")
}

func (n *Follower) IsLeader() bool {
	return false
}

func (n *Follower) Autocommit(ctx context.Context, req *txn.AutocommitRequest) (*txn.Status, error) {
	if !req.IsSnapshotRead() {
		return nil, errors.NotLeader
	}

	return n.txn.Autocommit(ctx, req)
}

func (n *Follower) Prepare(context.Context, *txn.PrepareRequest) (*txn.Status, error) {
	return nil, errors.NotLeader
}

func (n *Follower) Commit(context.Context, *txn.CommitRequest) (*txn.Status, error) {
	return nil, errors.NotLeader
}

func (n *Follower) Abort(context.Context, *txn.AbortRequest) (*txn.Status, error) {
	return nil, errors.NotLeader
}

func (n *Follower) StoreDecision(context.Context, *txn.Decision) (*txn.Status, error) {
	return nil, errors.NotLeader
}

func (n *Follower) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	return n.streamer.StreamPartition(req, stream)
}
