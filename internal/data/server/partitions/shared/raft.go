package shared

import (
	"fmt"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/hashicorp/raft"
)

func CreateRaft(multiraft *multiraft.MultiRaft, groupID, localID string, fsm multiraft.FSM, servers ...raft.Server) (*multiraft.Raft, error) {
	hasState, err := multiraft.HasExistingState(groupID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to determine Raft group state.")
	}

	raft, err := multiraft.New(groupID, fsm, raft.ServerID(localID))
	if err != nil {
		return nil, errors.Wrap(err, "failed to start Raft.")
	}

	if hasState || len(servers) == 0 {
		return raft, nil
	}

	if err := raft.Bootstrap(servers...); err != nil {
		raft.Stop()
		return nil, errors.Wrap(err, "bootstrap Raft group failed.")
	}

	// later, the partition leader will take ownership over the group and
	// update Raft configuration to match the partition config.

	return raft, nil
}

// A replica needs to receive two pieces of info:
//   - 1. the partition config: which gives the list of group replicas and their server IDs.
//   - 2. the data servers list: which gives the current address of each data server ID.
//   - if either is missing, the replica cannot be started, and
//   - because these are received independently in separate events, as per design, they
//     could become out of sync resulting in the same situation (e.g. a new server is
//     registered in the cluster and then added to the partition replica list. If the
//     local server receives first the config, containing the new replica, it will need to
//     wait for the data server list before being able to sync).
//   - The events are kept separate because they convey information with different
//     characteristics: data server config is expected to change slowly, while
//     the server address list could change with every data server restart cycle.
func TryMakeRaftServerList(config *control.DataServerConfig_Partition, dataServers *control.DataServers) []raft.Server {
	if config == nil || dataServers == nil {
		return nil
	}

	servers := make([]raft.Server, 0, len(config.Replicas))

	for _, replica := range config.Replicas {
		var suffrage raft.ServerSuffrage

		switch replica.State {
		case control.DataServerConfig_Joining, control.DataServerConfig_NonVoter:
			suffrage = raft.Nonvoter
		case control.DataServerConfig_Voter:
			suffrage = raft.Voter
		case control.DataServerConfig_Leaving:
			// skip the leaving replica which results in it being removed from Raft configuration by the group leader
			continue
		default:
			panic(fmt.Sprintf("unhandled replica.State %s", replica.State))
		}

		dataServer, ok := dataServers.Servers[replica.ServerId]
		if !ok {
			return nil
		}

		servers = append(servers, raft.Server{
			ID:       raft.ServerID(replica.Name),
			Address:  raft.ServerAddress(dataServer.Address),
			Suffrage: suffrage,
		})
	}

	return servers
}
