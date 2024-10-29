package storage

import (
	"fmt"

	"github.com/bcrusu/scout/internal/control"
)

func (f *FSM) nextServerID() uint64 {
	f.servers.LastId++
	return f.servers.LastId
}

func (f *FSM) isValidDataServer(serverID uint64) bool {
	server, ok := f.servers.Items[serverID]
	return ok && server.Type == control.ServerType_Data
}

func (f *FSM) isValidPartitionID(pid uint32) bool {
	return pid < f.partitionCount
}

func (f *FSM) isValidReplicaName(pid uint32, replica string) bool {
	part := f.partitions.Items[pid]
	_, ok := part.Replicas[replica]
	return ok
}

func (f *FSM) getReplica(pid uint32, replica string) *control.Partition_Replica {
	return f.partitions.Items[pid].Replicas[replica]
}

func (f *FSM) getReplicaByServer(pid uint32, serverID uint64) *control.Partition_Replica {
	return f.partitions.Items[pid].ReplicaForServer(serverID)
}

func (f *FSM) nextReplicaName(pid uint32) string {
	part := f.partitions.Items[pid]
	part.LastReplicaId++
	return fmt.Sprintf("p%d_r%d", part.Id, part.LastReplicaId)
}

func (f *FSM) isValidReplicaTransition(pid uint32, replicaName string, nextState control.ReplicaState) bool {
	replica := f.getReplica(pid, replicaName)

	switch nextState {
	case control.ReplicaState_Joining:
		return replica == nil
	case control.ReplicaState_Voter:
		return replica != nil && (replica.State == control.ReplicaState_Joining || replica.State == control.ReplicaState_NonVoter)
	case control.ReplicaState_NonVoter:
		return replica != nil && (replica.State == control.ReplicaState_Joining || replica.State == control.ReplicaState_Voter)
	case control.ReplicaState_Leaving:
		return replica != nil
	default:
		return false
	}
}
