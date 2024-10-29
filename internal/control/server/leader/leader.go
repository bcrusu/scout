package leader

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/common"
	"github.com/bcrusu/scout/internal/control/server/leader/partitions"
	"github.com/bcrusu/scout/internal/control/server/leader/sessions"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_    control.ServiceServer = (*Leader)(nil)
	_    utils.Lifecycle       = (*Leader)(nil)
	logL                       = logging.New("leader")
)

// Leader implements the Leader role.
type Leader struct {
	control.UnsafeServiceServer
	*common.Shared
	store             storage.Store
	sessionTracker    *sessions.Tracker
	partitionAssigner *partitions.Assigner
	components        []utils.Lifecycle
}

func New(store storage.Store) *Leader {
	l := &Leader{
		Shared:            common.New(store),
		store:             store,
		sessionTracker:    sessions.NewTracker(store),
		partitionAssigner: partitions.NewAssigner(store),
	}

	l.components = []utils.Lifecycle{
		l.sessionTracker,
		l.partitionAssigner,
	}

	return l
}

func (n *Leader) Start(ctx context.Context) error {
	utils.LifecycleStart(ctx, logL, n.components...)
	logL.Debug(ctx, "Started leader")
	return nil
}

func (n *Leader) Stop() {
	utils.LifecycleStop(logL.NoContext(), n.components...)
	logL.NoContext().Debug("Stopped leader")
}

func (n *Leader) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	cmd := &storage.Register{
		Type:    req.Type,
		Token:   req.Token,
		Address: req.Address,
	}

	result, err := n.store.Register(cmd)
	if err != nil {
		return nil, err
	}

	return &control.RegisterResponse{
		ServerId:   result.ServerID,
		ServerName: result.ServerName,
	}, nil
}

func (n *Leader) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
	return n.sessionTracker.NewSession(stream)
}
