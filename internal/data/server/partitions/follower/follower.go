package follower

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/common"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/errors"
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
	*common.Shared
	log   logging.Logger
	store storage.Store
}

func New(partitionID uint32, store storage.Store) *Follower {
	return &Follower{
		Shared: common.New(),
		log:    logging.WithComponent("follower").With("partition", partitionID),
		store:  store,
	}
}

func (n *Follower) Start(ctx context.Context) error {
	n.log.Debug(ctx, "Started")
	return nil
}

func (n *Follower) Stop() {
	n.log.NoContext().Debug("Stopped")
}

func (n *Follower) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	return nil, errors.NotLeader
}

func (n *Follower) Get(ctx context.Context, req *data.GetRequest) (*data.GetResponse, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, errors.InvalidRequest
	}

	value, ok := n.store.Get(req.Key)
	if !ok {
		return nil, errors.NotFound
	}

	return &data.GetResponse{
		Value: value,
	}, nil
}

func (n *Follower) Del(ctx context.Context, req *data.DelRequest) (*data.DelResponse, error) {
	return nil, errors.NotLeader
}
