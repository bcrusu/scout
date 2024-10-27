package follower

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/common"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_   control.ServiceServer = (*Follower)(nil)
	_   utils.Lifecycle       = (*Follower)(nil)
	log                       = logging.WithComponent("control_follower")
)

// Follower implements the follower role.
type Follower struct {
	control.UnsafeServiceServer
	*common.Shared
}

func New(store storage.Store) *Follower {
	return &Follower{
		Shared: common.New(store),
	}
}

func (n *Follower) Start(ctx context.Context) error {
	log.Debug(ctx, "Started follower")
	return nil
}

func (n *Follower) Stop() {
	log.NoContext().Debug("Stopped follower")
}

func (n *Follower) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	return nil, errors.NotLeader
}

func (n *Follower) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
	return errors.NotLeader
}
