package sessions

import (
	"fmt"
	"strconv"

	"github.com/bcrusu/scout/internal/control"
)

type dsConfigs map[uint64]*control.DataServerConfig
type asConfigs map[uint64]*control.ApiServerConfig
type dsPartMap map[uint32]*control.DataServerConfig_Partition

func (t *Tracker) makeDataServerConfigs(servers *control.Servers, partitions *control.Partitions) dsConfigs {
	byServer := map[uint64]dsPartMap{}

	for _, p := range partitions.Items {
		partition := &control.DataServerConfig_Partition{
			ETag:     strconv.FormatUint(p.AssignmentsVersion, 10),
			Id:       p.Id,
			Replicas: map[string]*control.DataServerConfig_Replica{},
		}

		for _, replica := range p.Replicas {
			partition.Replicas[replica.Name] = &control.DataServerConfig_Replica{
				Name:     replica.Name,
				ServerId: replica.ServerId,
				State:    replica.State,
			}
		}

		// could have used the same iteration above, but looks cleaner this way
		for _, replica := range p.Replicas {
			if byServer[replica.ServerId] == nil {
				byServer[replica.ServerId] = dsPartMap{}
			}

			byServer[replica.ServerId][partition.Id] = partition
		}
	}

	result := dsConfigs{}
	for id := range servers.DataServers() {
		partitions := byServer[id]

		etags := make([]string, 0, len(partitions))
		for id, part := range partitions {
			etags = append(etags, fmt.Sprintf("part %d:%s", id, part.ETag))
		}

		result[id] = &control.DataServerConfig{
			ETag:       makeETag(etags...),
			Partitions: partitions,
		}
	}

	return result
}

func (t *Tracker) makeApiServerConfigs(servers *control.Servers) asConfigs {
	result := asConfigs{}
	for id := range servers.ApiServers() {
		result[id] = &control.ApiServerConfig{
			ETag:           "API",
			PartitionCount: t.store.PartitionCount(),
		}
	}

	return result
}
