package server

import (
	"context"
	"sync/atomic"

	"github.com/bcrusu/scout/internal/api"
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service     = (*AdminService)(nil)
	_ api.AdminServer = (*AdminService)(nil)
	_ utils.Lifecycle = (*AdminService)(nil)
)

// AdminService represents the administration service.
type AdminService struct {
	api.UnsafeAdminServer
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
	s.cancelFunc = utils.RunAsync(ctx, s.mainLoop)
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
			servers := make([]string, 0, len(x.Servers))
			for _, s := range x.Servers {
				servers = append(servers, s.Address)
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
	} else {
		return disc, nil
	}
}
