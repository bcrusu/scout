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
		ItemsVersion:  1,
		Items:         map[uint64]*Server{},
		StatusVersion: 1,
		Status:        map[uint64]*ServerStatus{},
		Tokens:        map[string]uint64{},
	}

	for _, server := range cmd.Servers {
		if server.Id > f.servers.LastId {
			f.servers.LastId = server.Id
		}

		f.servers.Items[server.Id] = &Server{
			Version:      1,
			Id:           server.Id,
			Name:         server.Name,
			Type:         ServerType_Control,
			RegisteredAt: timestamppb.New(appendedAt),
		}

		f.servers.Status[server.Id] = &ServerStatus{
			Version:     1,
			LastSeen:    timestamppb.New(appendedAt),
			LastAddress: server.Address,
		}
	}

	f.partitions = &Partitions{
		ItemsVersion:  1,
		Items:         map[uint32]*Partition{},
		StatusVersion: 1,
		Status:        map[uint32]*PartitionStatus{},
	}

	for id := range cmd.PartitionCount {
		f.partitions.Items[id] = &Partition{
			Version:       1,
			Id:            id,
			Replicas:      map[string]*Partition_Replica{}, // will be updated live by the partition assignment component
			LastReplicaId: 0,
		}

		f.partitions.Status[id] = &PartitionStatus{
			Version:  1,
			Replicas: map[string]*PartitionStatus_Replica{},
		}
	}

	return &BootstrapResult{
		Success: true,
	}, nil
}
