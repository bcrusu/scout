package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/control"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (f *FSM) applyBootstrap(appendedAt time.Time, cmd *Bootstrap) (*BootstrapResult, error) {
	if f.clusterName != "" {
		return &BootstrapResult{Success: false}, nil
	}

	f.clusterName = cmd.ClusterName
	f.createdTime = appendedAt
	f.partitionCount = cmd.PartitionCount

	f.servers = &control.Servers{
		Version:         1,
		RegisterVersion: 1,
		Items:           map[uint64]*control.Server{},
		Tokens:          map[string]uint64{},
	}

	for _, server := range cmd.Servers {
		f.servers.LastId = max(f.servers.LastId, server.Id)

		f.servers.Items[server.Id] = &control.Server{
			Id:           server.Id,
			Name:         server.Name,
			Type:         control.ServerType_Control,
			RegisteredAt: timestamppb.New(appendedAt),
			LastSeen:     timestamppb.New(appendedAt),
			LastAddress:  server.Address,
		}
	}

	f.partitions = &control.Partitions{
		Items: map[uint32]*control.Partition{},
	}

	for id := range cmd.PartitionCount {
		f.partitions.Items[id] = &control.Partition{
			Id:       id,
			Replicas: map[string]*control.Partition_Replica{},
		}
	}

	return &BootstrapResult{
		Success: true,
	}, nil
}
