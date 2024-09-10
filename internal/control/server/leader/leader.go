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
	raft      *multiraft.Raft
	store     storage.Store
	commandCh chan command
	mainCtx   context.Context
	stopFunc  context.CancelFunc
}

func New(raft *multiraft.Raft, store storage.Store) *Leader {
	return &Leader{
		Shared:    common.New(raft, store),
		raft:      raft,
		store:     store,
		commandCh: make(chan command),
	}
}

func (n *Leader) Start(ctx context.Context) error {
	n.mainCtx, n.stopFunc = context.WithCancel(context.Background())
	go n.mainLoop()

	log.Debug(ctx, "Started leader")
	return nil
}

func (n *Leader) Stop(ctx context.Context) {
	n.stopFunc()
	log.Debug(ctx, "Stopped leader")
}

func (n *Leader) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	if req == nil || !storage.IsValidClusterName(req.ClusterName) || !storage.IsValidAddress(req.Address) ||
		!storage.IsValidToken(req.Token) || n.store.IsEmpty() || req.ClusterName != n.store.ClusterName() {
		return nil, errors.InvalidRequest
	}

	cmd := &storage.Register{
		Token:   req.Token,
		Address: req.Address,
	}

	switch req.Type {
	case control.RegisterRequest_Control:
		cmd.Type = storage.ServerType_Control
	case control.RegisterRequest_Data:
		cmd.Type = storage.ServerType_Data
	case control.RegisterRequest_Api:
		cmd.Type = storage.ServerType_Api
	default:
		log.Warnf(ctx, "Unknown server type %v", req.Type)
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
	hello, err := stream.Recv()
	if err != nil {
		return errors.Wrap(err, "new session failed before hello")
	}

	if hello == nil || hello.ClusterName != n.store.ClusterName() || hello.ServerId == 0 || hello.Address == "" {
		return errors.InvalidRequest
	}

	server := n.store.Server(hello.ServerId)
	if server == nil {
		return errors.NotRegistered
	}

	// TODO
	return nil
	//	session := newSession(stream)

	// if err := n.sendToMainLoop(sessionStart{
	// 	hello:   hello,
	// 	server:  server,
	// 	session: session,
	// }); err != nil {
	// 	return err
	// }

	// runErr := session.run(stream.Context())

	// n.sendToMainLoop(sessionEnd{
	// 	hello:   hello,
	// 	server:  server,
	// 	session: session,
	// })

	// return runErr
}

func (n *Leader) mainLoop() {
	sessions := map[*session]bool{}
	sessionsByServer := map[uint64]*session{}

	for {
		select {
		case cmd := <-n.commandCh:
			var result error

			switch x := cmd.payload.(type) {
			case sessionStarting:
				if existing := sessionsByServer[x.server.Id]; existing != nil {
					existing.close()
					delete(sessions, existing)
					delete(sessionsByServer, x.server.Id)
				}

				sessions[x.session] = true
				sessionsByServer[x.server.Id] = x.session
			case sessionEnded:
				delete(sessions, x.session)
			default:
				result = errors.Errorf("unknown command type %T", cmd)
			}

			cmd.resultCh <- result
		}
	}
}

func (n *Leader) sendToMainLoop(payload any) error {
	cmd := command{
		payload:  payload,
		resultCh: make(chan error),
	}

	n.commandCh <- cmd
	return <-cmd.resultCh
}
