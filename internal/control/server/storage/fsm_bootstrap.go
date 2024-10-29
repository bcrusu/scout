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
	f.partitionCount = cmd.PartitionCount

	f.servers = &Servers{
		Version:         1,
		RegisterVersion: 1,
		Items:           map[uint64]*Server{},
		Tokens:          map[string]uint64{},
	}

	for _, server := range cmd.Servers {
		f.servers.LastId = max(f.servers.LastId, server.Id)

		f.servers.Items[server.Id] = &Server{
			Id:           server.Id,
			Name:         server.Name,
			Type:         ServerType_Control,
			RegisteredAt: timestamppb.New(appendedAt),
			LastSeen:     timestamppb.New(appendedAt),
			LastAddress:  server.Address,
		}
	}

	f.partitions = &Partitions{
		Items: map[uint32]*Partition{},
	}

	for id := range cmd.PartitionCount {
		f.partitions.Items[id] = &Partition{
			Id:       id,
			Replicas: map[string]*Partition_Replica{},
		}
	}

	return &BootstrapResult{
		Success: true,
	}, nil
}
