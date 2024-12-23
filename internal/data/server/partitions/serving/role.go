package serving

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ data.ServiceServer = (*role)(nil)
	_ utils.Lifecycle    = (*role)(nil)
)

type role struct {
	data.UnsafeServiceServer
	pid      uint32
	isLeader bool
	txn      *txn.Service
	streamer *streamer
}

func newRole(pid uint32, isLeader bool, db kv.DB, txn *txn.Service) *role {
	return &role{
		pid:      pid,
		isLeader: isLeader,
		txn:      txn,
		streamer: newStreamer(db),
	}
}

func (n *role) Start(ctx context.Context) error {
	if err := n.txn.Start(ctx); err != nil {
		return err
	}

	return nil
}

func (n *role) Stop() {
	n.txn.Stop()
}

func (n *role) IsLeader() bool {
	return n.isLeader
}

func (n *role) Autocommit(ctx context.Context, req *data.AutocommitRequest) (*data.TxnStatus, error) {
	return n.txn.Autocommit(ctx, req)
}

func (n *role) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	return n.txn.Prepare(ctx, req)
}

func (n *role) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	return n.txn.Commit(ctx, req)
}

func (n *role) Abort(ctx context.Context, req *data.AbortRequest) (*data.TxnStatus, error) {
	return n.txn.Abort(ctx, req)
}

func (n *role) StoreDecision(ctx context.Context, dec *data.Decision) (*data.TxnStatus, error) {
	return n.txn.StoreDecision(ctx, dec)
}

func (n *role) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	return n.streamer.StreamPartition(req, stream)
}
