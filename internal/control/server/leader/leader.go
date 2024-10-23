package leader

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/common"
	"github.com/bcrusu/scout/internal/control/server/convert"
	"github.com/bcrusu/scout/internal/control/server/leader/partitions"
	"github.com/bcrusu/scout/internal/control/server/leader/sessions"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
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
	raft              *multiraft.Raft
	store             storage.Store
	sessionTracker    *sessions.Tracker
	partitionAssigner *partitions.Assigner
	components        []utils.Lifecycle
}

func New(raft *multiraft.Raft, store storage.Store) *Leader {
	l := &Leader{
		Shared:            common.New(raft, store),
		raft:              raft,
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
	if req == nil || !storage.IsValidAddress(req.Address) || !storage.IsValidToken(req.Token) {
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
	// to recover the original id/name pair assigned above by the store in the
	// initial request.
	server := raft.Server{
		ID:       raft.ServerID(result.ServerName),
		Address:  raft.ServerAddress(req.Address),
		Suffrage: raft.Voter,
	}

	if cmd.Type == storage.ServerType_Control {
		if err := n.raft.AddOrUpdateServer(server); err != nil {
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
