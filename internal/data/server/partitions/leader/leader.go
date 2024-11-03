package leader

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ data.ServiceServer   = (*Leader)(nil)
	_ txn.TxnServiceServer = (*Leader)(nil)
	_ utils.Lifecycle      = (*Leader)(nil)
)

// Leader implements the Leader role.
type Leader struct {
	data.UnsafeServiceServer
	txn.UnsafeTxnServiceServer
	pid      uint32
	log      logging.Logger
	txn      *txn.Service
	streamer *shared.PartitionStreamer
}

func New(pid uint32, db kv.DB, txn *txn.Service) *Leader {
	return &Leader{
		pid:      pid,
		log:      logging.New("leader").With("partition", pid),
		txn:      txn,
		streamer: shared.NewPartitionStreamer(db),
	}
}

func (n *Leader) Start(ctx context.Context) error {
	if err := n.txn.Start(ctx); err != nil {
		return err
	}

	n.log.WithContext(ctx).Debug("Started leader")
	return nil
}

func (n *Leader) Stop() {
	n.txn.Stop()
	n.log.Debug("Stopped leader")
}

func (n *Leader) IsLeader() bool {
	return true
}

func (n *Leader) Autocommit(ctx context.Context, req *txn.AutocommitRequest) (*txn.Status, error) {
	return n.txn.Autocommit(ctx, req)
}

func (n *Leader) Prepare(ctx context.Context, req *txn.PrepareRequest) (*txn.Status, error) {
	return n.txn.Prepare(ctx, req)
}

func (n *Leader) Commit(ctx context.Context, req *txn.CommitRequest) (*txn.Status, error) {
	return n.txn.Commit(ctx, req)
}

func (n *Leader) Abort(ctx context.Context, req *txn.AbortRequest) (*txn.Status, error) {
	return n.txn.Abort(ctx, req)
}

func (n *Leader) StoreDecision(ctx context.Context, dec *txn.Decision) (*txn.Status, error) {
	return n.txn.StoreDecision(ctx, dec)
}

func (n *Leader) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	return n.streamer.StreamPartition(req, stream)
}
