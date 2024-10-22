package convert

import (
	"fmt"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/storage"
)

func FromServerType(in storage.ServerType) control.ServerType {
	switch in {
	case storage.ServerType_Control:
		return control.ServerType_Control
	case storage.ServerType_Data:
		return control.ServerType_Data
	case storage.ServerType_Api:
		return control.ServerType_Api
	default:
		panic(fmt.Sprintf("unhandled ServerType %s", in))
	}
}

func FromReplicaState(in storage.ReplicaState) control.DataServerConfig_ReplicaState {
	switch in {
	case storage.ReplicaState_Joining:
		return control.DataServerConfig_Joining
	case storage.ReplicaState_Voter:
		return control.DataServerConfig_Voter
	case storage.ReplicaState_NonVoter:
		return control.DataServerConfig_NonVoter
	case storage.ReplicaState_Leaving:
		return control.DataServerConfig_Leaving
	default:
		panic(fmt.Sprintf("unhandled Partition_ReplicaState %s", in))
	}
}
