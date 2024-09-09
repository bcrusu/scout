package follower

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/common"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   data.DataServer = (*Follower)(nil)
	_   utils.Lifecycle = (*Follower)(nil)
	log                 = logging.WithComponent("data_follower")
)

// Follower implements the follower role.
type Follower struct {
	data.UnsafeDataServer
	*common.Shared
	raft *multiraft.Raft
}

func New(raft *multiraft.Raft) *Follower {
	return &Follower{
		Shared: common.New(raft),
		raft:   raft,
	}
}

func (n *Follower) Start(ctx context.Context) error {
	log.Debug(ctx, "Started follower")
	return nil
}

func (n *Follower) Stop(ctx context.Context) {
	log.Debug(ctx, "Stopped follower")
}

func (n *Follower) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	return nil, errors.NotLeader
}

func (n *Follower) Get(ctx context.Context, req *data.GetRequest) (*data.GetResponse, error) {
	// TODO: allow read from follower
	return nil, errors.NotLeader
}

func (n *Follower) Delete(ctx context.Context, req *data.DeleteRequest) (*data.DeleteResponse, error) {
	return nil, errors.NotLeader
}
