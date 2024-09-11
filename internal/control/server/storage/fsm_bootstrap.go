package storage

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func (f *FSM) applyBootstrap(appendedAt time.Time, cmd *Bootstrap) (*BootstrapResult, error) {
	if f.clusterName != "" {
		return &BootstrapResult{Success: false}, nil
	}

	f.clusterName = cmd.ClusterName
	f.clusterCreatedTime = appendedAt
	f.servers = &Servers{Version: 1}

	for _, server := range cmd.Servers {
		if server.Id > f.servers.LastServerId {
			f.servers.LastServerId = server.Id
		}

		f.servers.Items[server.Id] = &Server{
			Version:     1,
			Id:          server.Id,
			Name:        server.Name,
			Type:        ServerType_Control,
			FirstSeen:   timestamppb.New(appendedAt),
			LastSeen:    timestamppb.New(appendedAt),
			LastAddress: server.Address,
		}
	}

	return &BootstrapResult{
		Success: true,
	}, nil
}
