package server

import (
	"context"
	"sync"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/follower"
	"github.com/bcrusu/graph/internal/data/server/leader"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service     = (*DataService)(nil)
	_ utils.Lifecycle = (*DataService)(nil)
)

// DataService represents the data service.
type DataService struct {
	data.UnimplementedServiceServer
	raft       *multiraft.MultiRaft
	store      storage.Store
	cancelFunc context.CancelFunc
	rolesLock  sync.RWMutex
	roles      map[uint]role // maps partition to role
}

type role interface {
	data.ServiceServer
	utils.Lifecycle
}

// NewDataService returns a new DataService instance
func NewDataService(raft *multiraft.MultiRaft, store storage.Store) *DataService {
	return &DataService{
		raft:  raft,
		store: store,
		roles: make(map[uint]role),
	}
}

func (s *DataService) RegisterToServer(server *grpc.Server) {
	data.RegisterServiceServer(server, s)
}

func (s *DataService) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(s.mainLoop)

	s.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (s *DataService) Stop(ctx context.Context) {
	s.cancelFunc()
}

// TODO: handle also the case when a raft group is removed from this server
func (s *DataService) mainLoop(ctx context.Context) {
	for {
		select {
		case x := <-s.raft.GetLeaderChan():
			var old role
			var new role

			if x.IsLeader {
				new = leader.New(x.Raft, s.store)
			} else {
				new = follower.New(x.Raft)
			}

			s.rolesLock.Lock()
			old = s.roles[x.GroupID]
			s.roles[x.GroupID] = nil
			s.rolesLock.Unlock()

			old.Stop(ctx) // TODO: drain in-flight requests?

			if err := new.Start(ctx); err != nil {
				log.WithError(err).Errorf(ctx, "Failed to start %T. Shutting down...", new)
				utils.LifecycleShutdown(ctx)
				return
			}

			s.rolesLock.Lock()
			s.roles[x.GroupID] = new
			s.rolesLock.Unlock()
		case <-ctx.Done():
			s.rolesLock.Lock()
			roles := s.roles
			s.roles = map[uint]role{}
			s.rolesLock.Unlock()

			for _, role := range roles {
				role.Stop(ctx)
			}

			return
		}
	}
}

func (s *DataService) getRole() (role, error) {
	var groupID uint // TODO

	s.rolesLock.RLock()
	role, ok := s.roles[groupID]
	s.rolesLock.RUnlock()

	if !ok || role == nil {
		return nil, errors.Unavailable
	}
	return role, nil
}

func (s *DataService) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Set(ctx, req)
	}
}

func (s *DataService) Get(ctx context.Context, req *data.GetRequest) (*data.GetResponse, error) {
	if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Get(ctx, req)
	}
}

func (s *DataService) Del(ctx context.Context, req *data.DelRequest) (*data.DelResponse, error) {
	if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Del(ctx, req)
	}
}
