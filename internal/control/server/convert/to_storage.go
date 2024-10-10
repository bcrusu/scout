package convert

import (
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/storage"
)

func ToServerType(in control.ServerType) storage.ServerType {
	switch in {
	case control.ServerType_Control:
		return storage.ServerType_Control
	case control.ServerType_Data:
		return storage.ServerType_Data
	case control.ServerType_Api:
		return storage.ServerType_Api
	default:
		return storage.ServerType_Unknown
	}
}

func ToPartitionJoiningStatus(in *control.DataServerStatus_JoiningStatus) *storage.PartitionStatus_JoiningStatus {
	if in == nil {
		return nil
	}

	return &storage.PartitionStatus_JoiningStatus{
		Completed: in.Completed,
	}
}

func ToPartitionLeavingStatus(in *control.DataServerStatus_LeavingStatus) *storage.PartitionStatus_LeavingStatus {
	if in == nil {
		return nil
	}

	return &storage.PartitionStatus_LeavingStatus{
		Completed: in.Completed,
	}
}
