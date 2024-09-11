package leader

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/common"
	"github.com/bcrusu/graph/internal/control/server/convert"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_    control.ServiceServer = (*Leader)(nil)
	_    utils.Lifecycle       = (*Leader)(nil)
	logL                       = logging.WithComponent("control_leader")
)

// Leader implements the Leader role.
type Leader struct {
	control.UnsafeServiceServer
	*common.Shared
	raft           *multiraft.Raft
	store          storage.Store
	sessionTracker *sessionTracker
}

func New(raft *multiraft.Raft, store storage.Store) *Leader {
	return &Leader{
		Shared:         common.New(raft, store),
		raft:           raft,
		store:          store,
		sessionTracker: newSessionTracker(store),
	}
}

func (n *Leader) Start(ctx context.Context) error {
	utils.LifecycleStart(ctx, logL, n.sessionTracker)
	logL.Debug(ctx, "Started leader")
	return nil
}

func (n *Leader) Stop(ctx context.Context) {
	utils.LifecycleStop(ctx, logL, n.sessionTracker)
	logL.Debug(ctx, "Stopped leader")
}

func (n *Leader) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	if req == nil || !storage.IsValidClusterName(req.ClusterName) || !storage.IsValidAddress(req.Address) ||
		!storage.IsValidToken(req.Token) || n.store.IsEmpty() || req.ClusterName != n.store.ClusterName() {
		return nil, errors.InvalidRequest
	}

	cmd := &storage.Register{
		Type:    convert.ToServerType(req.Type),
		Token:   req.Token,
		Address: req.Address,
	}

	if cmd.Type == storage.ServerType_Unknown {
		logL.Errorf(ctx, "Received unknown server type %v from server at %s", req.Type, req.Address)
		return nil, errors.InvalidRequest
	}

	result, err := n.store.Register(cmd)
	if err != nil {
		return nil, err
	}

	// For control plane servers, add them immediately to the Raft group.
	// If the operation fails, the server can retry later with the same token
	// to recover the original id/name pair assigned above by the FSM in the
	// initial request.
	if cmd.Type == storage.ServerType_Control {
		if err := n.raft.AddVoter(ctx, result.ServerName, req.Address); err != nil {
			return nil, err
		}
	}

	return &control.RegisterResponse{
		ServerId:   result.ServerID,
		ServerName: result.ServerName,
	}, nil
}

func (n *Leader) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
	return n.sessionTracker.NewSession(stream)
}
