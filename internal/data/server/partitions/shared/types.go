package shared

import (
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/utils"
)

type Service interface {
	data.ServiceServer
	txn.TxnServiceServer
	IsLeader() bool
}

type Replica interface {
	utils.Lifecycle
	GetService() Service
	GetStatus() *control.DataServerStatus_Replica
}
