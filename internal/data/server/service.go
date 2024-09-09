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
	data.UnimplementedDataServer
	raft      *multiraft.MultiRaft
	store     storage.Store
	stopCh    chan any
	rolesLock sync.Mutex
	roles     map[uint]role // maps partition to role
}

type role interface {
	data.DataServer
	utils.Lifecycle
}

// NewDataService returns a new DataService instance
func NewDataService(raft *multiraft.MultiRaft, store storage.Store) *DataService {
	return &DataService{
		raft:   raft,
		store:  store,
		stopCh: make(chan any),
		roles:  make(map[uint]role),
	}
}

func (s *DataService) RegisterToServer(server *grpc.Server) {
	data.RegisterDataServer(server, s)
}

func (s *DataService) Start(ctx context.Context) error {
	go s.watchLeaderChan(ctx)
	return nil
}

func (s *DataService) Stop(ctx context.Context) {
	close(s.stopCh)
}

func (s *DataService) watchLeaderChan(ctx context.Context) {
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
			s.roles[x.GroupID] = new
			s.rolesLock.Unlock()

			if err := new.Start(ctx); err != nil {
				log.WithError(err).Errorf(ctx, "Failed to start %T. Shutting down...", new)
				panic("TODO: trigger server shutdown")
			}

			old.Stop(ctx) // TODO: drain in-flight requests?
		case <-s.stopCh:
			return
		}
	}
}

func (s *DataService) getRole() (role, error) {
	var groupID uint // TODO

	s.rolesLock.Lock()
	role, ok := s.roles[groupID]
	s.rolesLock.Unlock()

	if !ok {
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

func (s *DataService) Delete(ctx context.Context, req *data.DeleteRequest) (*data.DeleteResponse, error) {
	if role, err := s.getRole(); err != nil {
		return nil, err
	} else {
		return role.Delete(ctx, req)
	}
}
