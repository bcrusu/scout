package leader

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/common"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_   control.ControlServer = (*Leader)(nil)
	_   utils.Lifecycle       = (*Leader)(nil)
	log                       = logging.WithComponent("control_leader")
)

// Leader implements the Leader role.
type Leader struct {
	control.UnsafeControlServer
	*common.Shared
	raft  *multiraft.Raft
	store storage.Store
}

func New(raft *multiraft.Raft, store storage.Store) *Leader {
	return &Leader{
		Shared: common.New(raft),
		raft:   raft,
		store:  store,
	}
}

func (n *Leader) Start(ctx context.Context) error {
	log.Debug(ctx, "Started leader")
	return nil
}

func (n *Leader) Stop(ctx context.Context) {
	log.Debug(ctx, "Stopped leader")
}

func (n *Leader) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	if req == nil || req.ClusterName == "" || len(req.ClusterName) > storage.MaxClusterNameLen || req.Address == "" ||
		len(req.Address) > storage.MaxAddressLen || req.Token == "" || len(req.Token) > storage.MaxTokenLen || req.Payload == nil ||
		req.ClusterName != n.store.ClusterName() {
		return nil, errors.InvalidRequest
	}

	cmd := &storage.Register{
		Token:   req.Token,
		Address: req.Address,
	}

	switch req.Payload.(type) {
	case *control.RegisterRequest_Control:
		cmd.Type = storage.ServerType_Control
	case *control.RegisterRequest_Data:
		cmd.Type = storage.ServerType_Data
	case *control.RegisterRequest_Api:
		cmd.Type = storage.ServerType_Api
	default:
		log.Warnf(ctx, "Unknown register request type %T", req.Payload)
		return nil, errors.InvalidRequest
	}

	result, err := storage.ApplyR[storage.RegisterResult](n.raft, cmd)
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
	return nil // TODO
}
