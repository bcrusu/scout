package shared

import (
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/utils"
)

type Service interface {
	data.ServiceServer
	IsLeader() bool
}

type Replica interface {
	utils.Lifecycle
	GetService() Service
	GetStatus() *control.DataServerStatus_Replica
}
