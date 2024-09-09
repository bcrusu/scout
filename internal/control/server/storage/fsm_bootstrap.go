package storage

import (
	"time"
)

func (f *FSM) applyBootstrap(appendedAt time.Time, cmd *Bootstrap) (*BootstrapResult, error) {
	if !f.IsEmpty() {
		return &BootstrapResult{Success: false}, nil
	}

	f.clusterName = cmd.ClusterName
	f.createdTime = appendedAt

	for _, server := range cmd.Servers {
		id := server.Id

		if id > f.lastServerID {
			f.lastServerID = id
		}

		f.serverNames[id] = server.Name
		f.serverFirstSeen[id] = appendedAt
		f.serverLastSeen[id] = appendedAt
	}

	return &BootstrapResult{
		Success: true,
	}, nil
}
