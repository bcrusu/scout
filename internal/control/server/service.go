package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/follower"
	"github.com/bcrusu/scout/internal/control/server/leader"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ rpc.Service           = (*ControlService)(nil)
	_ control.ServiceServer = (*ControlService)(nil)
	_ utils.Lifecycle       = (*ControlService)(nil)
)

// ControlService represents the control plane service
type ControlService struct {
	control.UnsafeServiceServer
	store      storage.Store
	cancelFunc context.CancelFunc
	role       atomic.Pointer[roleDrainer]
}

type role interface {
	control.ServiceServer
	utils.Lifecycle
}

// NewControlService returns a new ControlService instance
func NewControlService(store storage.Store) *ControlService {
	return &ControlService{
		store: store,
	}
}

func (s *ControlService) RegisterToServer(server *grpc.Server) {
	control.RegisterServiceServer(server, s)
}

func (s *ControlService) Start(ctx context.Context) error {
	s.cancelFunc = utils.RunAsync(ctx, s.mainLoop)
	return nil
}

func (s *ControlService) Stop() {
	s.cancelFunc()
}

func (s *ControlService) mainLoop(ctx context.Context) {
	if !s.waitBootstrapped(ctx) {
		return
	}

	isLeader := s.store.Raft().IsLeader()

	if !s.setRole(ctx, isLeader) {
		return
	}

	for {
		select {
		case next := <-s.store.Raft().LeaderChan():
			if next != isLeader && !s.setRole(ctx, next) {
				return
			}
			isLeader = next
		case <-ctx.Done():
			if old := s.role.Swap(nil); old != nil {
				old.Stop()
			}
			return
		}
	}
}

func (s *ControlService) waitBootstrapped(ctx context.Context) bool {
	if s.store.Bootstrapped() {
		return true
	}

	ticker := time.NewTicker(time.Second / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if s.store.Bootstrapped() {
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}

func (s *ControlService) setRole(ctx context.Context, isLeader bool) bool {
	// Setting to nil will reject new incoming requests with Unavailable error
	// until the new role is ready. Could use and intermediary role type
	// that retries, with backoff, until the new role is ready. Will leave it,
	// for now, to the client to retry the request.
	if old := s.role.Swap(nil); old != nil {
		go old.Stop()
	}

	var new role
	if isLeader {
		new = leader.New(s.store)
	} else {
		new = follower.New(s.store)
	}

	drainer := newRoleDrainer(new)

	if err := drainer.Start(ctx); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("Failed to start role %T. Shutting down...", new)
		utils.GracefulShutdown("Failed to start role.")
		return false
	}

	s.role.Store(drainer)
	return true
}

func (s *ControlService) getRole() (role, error) {
	v := s.role.Load()
	if v == nil {
		return nil, errors.Unavailable
	}
	return v, nil
}

func (s *ControlService) Discover(ctx context.Context, req *emptypb.Empty) (*control.DiscoverResponse, error) {
	if !s.store.Bootstrapped() {
		return nil, errors.Unavailable
	} else if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Discover(ctx, req)
	}
}

func (s *ControlService) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	if !s.store.Bootstrapped() {
		return nil, errors.Unavailable
	} else if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Register(ctx, req)
	}
}

func (s *ControlService) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
	if !s.store.Bootstrapped() {
		return errors.Unavailable
	} else if role, err := s.getRole(); err != nil {
		return err
	} else {
		return role.NewSession(stream)
	}
}

func (s *ControlService) GetClusterInfo(ctx context.Context, req *emptypb.Empty) (*control.ClusterInfo, error) {
	if !s.store.Bootstrapped() {
		return nil, errors.Unavailable
	} else if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.GetClusterInfo(ctx, req)
	}
}
