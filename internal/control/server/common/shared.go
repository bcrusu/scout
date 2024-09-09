package common

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
)

var (
	log = logging.WithComponent("control_common")
)

// Shared implements common functionality for both leader and follower roles.
type Shared struct {
	raft              *multiraft.Raft
	serviceConfigJson string
}

func New(raft *multiraft.Raft) *Shared {
	scfg := serviceconfig.DefaultServiceConfig().WithLBGraphControl()

	return &Shared{
		raft:              raft,
		serviceConfigJson: scfg.ToJson(),
	}
}

// Discover is used early by control plane clients to discover the cluster servers.
// Can be invoked on leader and followers.
func (n *Shared) Discover(ctx context.Context, req *control.DiscoverRequest) (*control.DiscoverResponse, error) {
	// TODO: read state from FSM and validate cluster name.
	// This ensures that the cluster has completed bootstrapping process

	leaderID, _, err := n.raft.GetLeader()
	if err != nil {
		return nil, err
	}

	config, err := n.raft.GetConfiguration()
	if err != nil {
		return nil, err
	}

	servers := make([]*control.DiscoverResponse_Server, len(config.Servers))
	for i, server := range config.Servers {
		servers[i] = &control.DiscoverResponse_Server{
			Address: string(server.Address),
			Leader:  string(server.ID) == leaderID,
		}
	}

	log.Debug(ctx, "Discover success", "servers", servers)

	return &control.DiscoverResponse{
		Servers:           servers,
		ServiceConfigJson: n.serviceConfigJson,
	}, nil
}
