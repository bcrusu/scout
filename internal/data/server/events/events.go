package events

import "github.com/bcrusu/scout/internal/control"

type ReplicaStatus map[uint32]*control.DataServerStatus_Replica
