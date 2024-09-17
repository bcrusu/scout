package server

import (
	"context"
	"sync/atomic"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/follower"
	"github.com/bcrusu/graph/internal/control/server/leader"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service     = (*ControlService)(nil)
	_ utils.Lifecycle = (*ControlService)(nil)
)

// ControlService represents the control plane service
type ControlService struct {
	control.UnimplementedServiceServer
	raft       *multiraft.Raft
	store      storage.Store
	cancelFunc context.CancelFunc
	role       atomic.Value
}

type role interface {
	control.ServiceServer
	utils.Lifecycle
}

// NewControlService returns a new ControlService instance
func NewControlService(raft *multiraft.Raft, store storage.Store) *ControlService {
	return &ControlService{
		raft:  raft,
		store: store,
	}
}

func (s *ControlService) RegisterToServer(server *grpc.Server) {
	control.RegisterServiceServer(server, s)
}

func (s *ControlService) Start(ctx context.Context) error {
	// start as follower
	role := NewDrainerService(follower.New(s.raft, s.store))
	s.role.Store(role)

	if err := role.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start as follower")
	}

	mainLoop, cancelFunc := utils.WithCancelAndWait(s.mainLoop)

	s.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (s *ControlService) Stop() {
	s.cancelFunc()
}

func (s *ControlService) mainLoop(ctx context.Context) {
	for {
		select {
		case isLeader := <-s.raft.GetLeaderChan():
			// Setting to nil will reject new incoming requests with Unavailable error
			// until the new role is ready. Could use and intermediary role type
			// that retries, with backoff, until the new role is ready. Will leave it,
			// for now, to the client to retry the request.
			old := s.role.Swap(nil).(*DrainerService)
			go old.Stop()

			var new role
			if isLeader {
				new = leader.New(s.raft, s.store)
			} else {
				new = follower.New(s.raft, s.store)
			}

			drainer := NewDrainerService(new)

			if err := drainer.Start(ctx); err != nil {
				log.WithError(err).Errorf(ctx, "Failed to start role %T. Shutting down...", new)
				utils.LifecycleShutdown(ctx)
				return
			}

			s.role.Store(drainer)
		case <-ctx.Done():
			old := s.role.Swap(nil).(*DrainerService)
			old.Stop()
			return
		}
	}
}

func (s *ControlService) getRole() (role, error) {
	v := s.role.Load()
	if v == nil {
		return nil, errors.Unavailable
	}
	return v.(role), nil
}

func (s *ControlService) Discover(ctx context.Context, req *control.DiscoverRequest) (*control.DiscoverResponse, error) {
	if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Discover(ctx, req)
	}
}

func (s *ControlService) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Register(ctx, req)
	}
}

func (s *ControlService) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
	if role, err := s.getRole(); err != nil {
		return err
	} else {
		return role.NewSession(stream)
	}
}
