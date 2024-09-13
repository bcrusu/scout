package convert

import (
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/storage"
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
		return control.ServerType_Unknown
	}
}
