package storage

func (f *FSM) nextServerID() uint64 {
	f.servers.LastId++
	return f.servers.LastId
}

func (f *FSM) isValidDataServer(serverID uint64) bool {
	server, ok := f.servers.Items[serverID]
	return ok && server.Type == ServerType_Data
}

func (f *FSM) isValidPartitionID(pid uint32) bool {
	return pid < f.partitionCount
}

func (f *FSM) isValidReplicaName(pid uint32, replica string) bool {
	part := f.partitions.Items[pid]
	_, ok := part.Replicas[replica]
	return ok
}

func (f *FSM) getPartition(pid uint32) *Partition {
	return f.partitions.Items[pid]
}

func (f *FSM) getReplicaByName(pid uint32, replica string) *Partition_Replica {
	return f.partitions.Items[pid].Replicas[replica]
}

func (f *FSM) getReplicaByServer(pid uint32, serverID uint64) *Partition_Replica {
	return f.partitions.Items[pid].getReplicaForServer(serverID)
}

func (f *FSM) isValidReplicaTransition(pid uint32, replicaName string, nextState ReplicaState) bool {
	replica := f.getReplicaByName(pid, replicaName)

	switch nextState {
	case ReplicaState_Joining:
		return replica == nil
	case ReplicaState_Voter:
		return replica != nil && (replica.State == ReplicaState_Joining || replica.State == ReplicaState_NonVoter)
	case ReplicaState_NonVoter:
		return replica != nil && (replica.State == ReplicaState_Joining || replica.State == ReplicaState_Voter)
	case ReplicaState_Leaving:
		return replica != nil
	default:
		return false
	}
}
