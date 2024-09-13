package storage

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	groupNamePrefix = "part_"
)

func (f *FSM) applyBootstrap(appendedAt time.Time, cmd *Bootstrap) (*BootstrapResult, error) {
	if f.clusterName != "" {
		return &BootstrapResult{Success: false}, nil
	}

	f.clusterName = cmd.ClusterName
	f.clusterCreatedTime = appendedAt
	f.servers = &Servers{
		Version: 1,
		Items:   map[uint64]*Server{},
	}

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

	f.partitionCount = cmd.PartitionCount
	f.partitions = &Partitions{
		Version: 1,
		Items:   map[uint32]*Partition{},
	}

	for id := range cmd.PartitionCount {
		f.partitions.Items[id] = &Partition{
			Version:      1,
			Id:           id,
			GroupName:    fmt.Sprintf("%s%d", groupNamePrefix, id),
			GroupMembers: []*Partition_Member{}, // will be updated live by the partition assignment component
			LastMemberId: 0,
		}
	}

	return &BootstrapResult{
		Success: true,
	}, nil
}
