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
	control.UnimplementedControlServer
	raft     *multiraft.Raft
	store    storage.Store
	stopCh   chan any
	roleLock sync.Mutex
	role     role
}

type role interface {
	control.ControlServer
	utils.Lifecycle
}

// NewControlService returns a new ControlService instance
func NewControlService(raft *multiraft.Raft, store storage.Store) *ControlService {
	return &ControlService{
		raft:   raft,
		store:  store,
		stopCh: make(chan any),
		role:   follower.New(raft, store), // always start as follower
	}
}

func (s *ControlService) RegisterToServer(server *grpc.Server) {
	control.RegisterControlServer(server, s)
}

func (s *ControlService) Start(ctx context.Context) error {
	if err := s.role.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start as follower")
	}

	go s.watchLeaderChan(ctx)
	return nil
}

func (s *ControlService) Stop(ctx context.Context) {
	close(s.stopCh)
}

func (s *ControlService) watchLeaderChan(ctx context.Context) {
	for {
		select {
		case isLeader := <-s.raft.GetLeaderChan():
			var old role
			var new role

			if isLeader {
				new = leader.New(s.raft, s.store)
			} else {
				new = follower.New(s.raft, s.store)
			}

			s.roleLock.Lock()
			old = s.role
			s.role = new
			s.roleLock.Unlock()

			if err := new.Start(ctx); err != nil {
				log.WithError(err).Errorf(ctx, "Failed to start role %T. Shutting down...", new)
				panic("TODO: trigger server shutdown")
			}

			old.Stop(ctx) // TODO: drain in-flight requests?
		case <-s.stopCh:
			return
		}
	}
}

func (s *ControlService) getRole() (role, error) {
	s.roleLock.Lock()
	role := s.role
	s.roleLock.Unlock()

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
