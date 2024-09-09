package follower

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/common"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_   control.ControlServer = (*Follower)(nil)
	_   utils.Lifecycle       = (*Follower)(nil)
	log                       = logging.WithComponent("control_follower")
)

// Follower implements the follower role.
type Follower struct {
	control.UnsafeControlServer
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

func (n *Follower) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	return nil, errors.NotLeader
}

func (n *Follower) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
	return errors.NotLeader
}
