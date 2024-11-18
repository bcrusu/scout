package server

import (
	"context"
	"sync/atomic"

	"github.com/bcrusu/scout/internal/api"
	"github.com/bcrusu/scout/internal/api/server/config"
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ rpc.Service       = (*ApiService)(nil)
	_ api.ServiceServer = (*ApiService)(nil)
	_ utils.Lifecycle   = (*ApiService)(nil)
)

// ApiService represents the administration service.
type ApiService struct {
	api.UnsafeServiceServer
	config     config.Config
	id         identity.Identity
	cancelFunc context.CancelFunc
	discover   atomic.Pointer[api.DiscoverResponse]
}

// NewApiService returns a new ApiService instance
func NewApiService(id identity.Identity) *ApiService {
	return &ApiService{
		config: config.Get(),
		id:     id,
	}
}

func (s *ApiService) RegisterToServer(server *grpc.Server) {
	api.RegisterServiceServer(server, s)
}

func (s *ApiService) Start(ctx context.Context) error {
	s.cancelFunc = utils.RunAsync(ctx, s.mainLoop)
	return nil
}

func (s *ApiService) Stop() {
	s.cancelFunc()
}

func (s *ApiService) mainLoop(ctx context.Context) {
	apiServersSub := eventbus.Subscribe[*control.ApiServers]()
	defer apiServersSub.Unsubscribe()

	for {
		select {
		case x := <-apiServersSub.Items():
			var servers []string

			if !s.config.ProxyMode {
				servers = make([]string, 0, len(x.Servers))
				for _, s := range x.Servers {
					servers = append(servers, s.Address)
				}
			}

			s.discover.Store(&api.DiscoverResponse{
				ETag:              x.ETag,
				ProxyMode:         s.config.ProxyMode,
				Servers:           servers,
				ServiceConfigJson: x.ServiceConfigJson,
			})
		case <-ctx.Done():
			return
		}
	}
}

func (s *ApiService) Discover(ctx context.Context, req *api.DiscoverRequest) (*api.DiscoverResponse, error) {
	if disc := s.discover.Load(); disc == nil {
		return nil, errors.Unavailable
	} else {
		return disc, nil
	}
}
