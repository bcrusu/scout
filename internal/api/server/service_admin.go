package server

import (
	"context"
	"sync/atomic"

	"github.com/bcrusu/graph/internal/api"
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service     = (*AdminService)(nil)
	_ utils.Lifecycle = (*AdminService)(nil)
)

// AdminService represents the administration service.
type AdminService struct {
	api.UnimplementedAdminServer
	id         identity.Identity
	cancelFunc context.CancelFunc
	discover   atomic.Pointer[api.DiscoverResponse]
}

// NewAdminService returns a new AdminService instance
func NewAdminService(id identity.Identity) *AdminService {
	return &AdminService{
		id: id,
	}
}

func (s *AdminService) RegisterToServer(server *grpc.Server) {
	api.RegisterAdminServer(server, s)
}

func (s *AdminService) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(s.mainLoop)

	s.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (s *AdminService) Stop() {
	s.cancelFunc()
}

func (s *AdminService) mainLoop(ctx context.Context) {
	apiServersSub := eventbus.Subscribe[*control.ApiServers]()
	defer apiServersSub.Unsubscribe()

	for {
		select {
		case x := <-apiServersSub.Items():
			servers := make([]string, len(x.Servers))
			for i, s := range x.Servers {
				servers[i] = s.Address
			}

			s.discover.Store(&api.DiscoverResponse{
				ETag:              x.ETag,
				Servers:           servers,
				ServiceConfigJson: x.ServiceConfigJson,
			})
		case <-ctx.Done():
			return
		}
	}
}

func (s *AdminService) Discover(ctx context.Context, req *api.DiscoverRequest) (*api.DiscoverResponse, error) {
	if disc := s.discover.Load(); disc == nil {
		return nil, errors.Unavailable
	} else if req.ClusterName != s.id.ClusterName {
		return nil, errors.PermissionDenied
	} else {
		return disc, nil
	}
}
