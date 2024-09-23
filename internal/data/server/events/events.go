package events

import "github.com/bcrusu/graph/internal/control"

type ReplicaStatus map[uint32]*control.DataServerStatus_Replica
