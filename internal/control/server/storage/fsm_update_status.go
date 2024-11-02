package storage

func (f *FSM) applyUpdateStatus(cmd *UpdateStatus) {
	for id, update := range cmd.Servers {
		server := f.servers.Items[id]
		if server == nil {
			continue
		}

		server.LastSeen = update.LastSeen
		server.LastAddress = update.LastAddress
	}

	for pid, pUpdate := range cmd.Partitions {
		part := f.partitions.Items[pid]

		part.Leader = pUpdate.Leader
		part.LeaderTerm = pUpdate.LeaderTerm
		part.LeaderAppliedIndex = pUpdate.LeaderAppliedIndex
		part.CommitedIndex = pUpdate.CommitedIndex

		for name, rUpdate := range pUpdate.Replicas {
			replica := part.Replicas[name]
			if replica == nil {
				continue
			}

			replica.LastUpdate = rUpdate.LastUpdate
			replica.AppliedIndex = rUpdate.AppliedIndex
			replica.Ready = rUpdate.Ready
		}
	}

	if len(cmd.Servers) > 0 {
		f.servers.Version++
	}

	if len(cmd.Partitions) > 0 {
		f.partitions.Version++
	}
}
