package follower

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/hlc"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ data.ServiceServer = (*Follower)(nil)
	_ utils.Lifecycle    = (*Follower)(nil)
)

// Follower implements the follower role.
type Follower struct {
	data.UnsafeServiceServer
	log   logging.Logger
	store storage.Store
}

func New(partitionID uint32, store storage.Store) *Follower {
	return &Follower{
		log:   logging.WithComponent("follower").With("partition", partitionID),
		store: store,
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

	cmd := &storage.TxnAutocommit{
		Timestamp: hlc.Now(),
		Txn:       txn,
	}

	return n.store.TxnAutocommit(cmd)
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
