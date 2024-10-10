package follower

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
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
	store    storage.Store
	streamer *shared.PartitionStreamer
}

func New(partitionID uint32, store storage.Store, db kv.DB) *Follower {
	return &Follower{
		log:      logging.WithComponent("follower").With("partition", partitionID),
		store:    store,
		streamer: shared.NewPartitionStreamer(db),
	}
}

func (n *Follower) Start(ctx context.Context) error {
	n.log.Debug(ctx, "Started")
	return nil
}

func (n *Follower) Stop() {
	n.log.NoContext().Debug("Stopped")
}

func (n *Follower) IsLeader() bool {
	return false
}

func (n *Follower) Autocommit(ctx context.Context, txn *data.Txn) (*data.TxnStatus, error) {
	if !txn.IsReplicaRead() {
		return nil, errors.NotLeader
	}

	return n.store.Autocommit(txn)
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

func (n *Follower) StoreDecision(context.Context, *data.TxnDecision) (*data.TxnStatus, error) {
	return nil, errors.NotLeader
}

func (n *Follower) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	return n.streamer.StreamPartition(req, stream)
}
