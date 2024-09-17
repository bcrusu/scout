package sessions

import (
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/storage"
)

type dsConfigs map[serverID]*control.DataServerConfig
type asConfigs map[serverID]*control.ApiServerConfig
type dsPartMap map[uint32]*control.DataServerConfig_Partition

func makeDataServerConfigs(servers *storage.Servers, partitions *storage.Partitions, oldConfigs dsConfigs) dsConfigs {
	byServer := map[serverID]dsPartMap{}

	for _, p := range partitions.Items {
		partition := &control.DataServerConfig_Partition{
			Version: p.Version,
			Id:      p.Id,
			Name:    p.Name,
			Members: make([]*control.DataServerConfig_Member, len(p.Members)),
		}

		for i, member := range p.Members {
			partition.Members[i] = &control.DataServerConfig_Member{
				Name:     member.Name,
				ServerId: member.ServerId,
				Voter:    member.Voter,
			}
		}

		// could have used the same iteration above, but looks cleaner this way
		for _, member := range p.Members {
			sid := serverID(member.ServerId)
			if byServer[sid] == nil {
				byServer[sid] = dsPartMap{}
			}

			byServer[sid][partition.Id] = partition
		}
	}

	getNextVersion := func(new, old *control.DataServerConfig) uint64 {
		if old == nil {
			return 1
		}

		if len(new.Partitions) != len(old.Partitions) {
			return old.Version + 1
		}

		for pid, newPart := range new.Partitions {
			if oldPart, ok := old.Partitions[pid]; !ok || newPart.Version != oldPart.Version {
				return old.Version + 1
			}
		}

		return old.Version
	}

	result := dsConfigs{}
	for _, server := range servers.ByType(storage.ServerType_Data) {
		sid := serverID(server.Id)
		old := oldConfigs[sid]

		new := &control.DataServerConfig{
			Partitions: byServer[sid],
		}

		new.Version = getNextVersion(new, old)
		result[sid] = new
	}

	return result
}

func makeApiServerConfigs(servers *storage.Servers, oldConfigs asConfigs) asConfigs {
	result := asConfigs{}
	for _, server := range servers.ByType(storage.ServerType_Api) {
		sid := serverID(server.Id)
		new := oldConfigs[sid]

		if new == nil {
			new = &control.ApiServerConfig{
				Version: 1,
			}
		}

		result[sid] = new
	}

	return result
}
