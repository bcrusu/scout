package server

import (
	"context"
	"sync"

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
	roleLock   sync.RWMutex
	role       role
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
		role:  follower.New(raft, store), // always start as follower
	}
}

func (s *ControlService) RegisterToServer(server *grpc.Server) {
	control.RegisterServiceServer(server, s)
}

func (s *ControlService) Start(ctx context.Context) error {
	if err := s.role.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start as follower")
	}

	mainLoop, cancelFunc := utils.WithCancelAndWait(s.mainLoop)

	s.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (s *ControlService) Stop(ctx context.Context) {
	s.cancelFunc()
}

func (s *ControlService) mainLoop(ctx context.Context) {
	for {
		select {
		case isLeader := <-s.raft.GetLeaderChan():
			var new role
			if isLeader {
				new = leader.New(s.raft, s.store)
			} else {
				new = follower.New(s.raft, s.store)
			}

			// will block new requests until the role is ready
			s.roleLock.Lock()
			s.role.Stop(ctx) // TODO: drain in-flight requests?

			if err := new.Start(ctx); err != nil {
				log.WithError(err).Errorf(ctx, "Failed to start role %T. Shutting down...", new)
				utils.LifecycleShutdown(ctx)
				return
			}

			s.role = new
			s.roleLock.Unlock()
		case <-ctx.Done():
			s.roleLock.Lock()
			old := s.role
			s.role = nil
			s.roleLock.Unlock()

			old.Stop(ctx)
			return
		}
	}
}

func (s *ControlService) getRole() (role, error) {
	s.roleLock.RLock()
	role := s.role
	s.roleLock.RUnlock()

	if role == nil {
		return nil, errors.Unavailable
	}
	return role, nil
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
