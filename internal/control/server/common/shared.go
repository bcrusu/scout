package common

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	log = logging.New("control_common")
)

// Shared implements common functionality for both leader and follower roles.
type Shared struct {
	store             storage.Store
	serviceConfigJson string
}

func New(store storage.Store) *Shared {
	config := config.Get().Service

	return &Shared{
		store:             store,
		serviceConfigJson: config.Control.GetServiceConfigJson(serviceconfig.LBNameScoutControl, control.Service_ServiceDesc),
	}
}

// Discover is used early by control plane clients to discover the cluster servers.
// Can be invoked on leader and followers.
func (n *Shared) Discover(ctx context.Context, _ *emptypb.Empty) (*control.DiscoverResponse, error) {
	raft := n.store.Raft()

	leaderID, _, ok := raft.GetLeader()
	if !ok {
		log.Debug(ctx, "Discover failed. Leader not available.")
		return nil, errors.Unavailable
	}

	raftServers, err := raft.GetServers()
	if err != nil {
		return nil, err
	}

	servers := make([]*control.DiscoverResponse_Server, len(raftServers))
	for i, server := range raftServers {
		servers[i] = &control.DiscoverResponse_Server{
			Address: string(server.Address),
			Leader:  server.ID == leaderID,
		}
	}

	log.Debug(ctx, "Discover success", "servers", servers)

	return &control.DiscoverResponse{
		Servers:           servers,
		ServiceConfigJson: n.serviceConfigJson,
	}, nil
}
