package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
)

func (f *FSM) applyUpdatePartitionStatus(_ time.Time, cmd *UpdatePartitionStatus) (*UpdateResult, error) {
	if cmd.IfMatch != 0 && cmd.IfMatch != f.partitions.StatusVersion {
		return nil, errors.FailedPrecondition
	}

	for id, update := range cmd.Items {
		status := f.partitions.Status[id]

		status.Version++
		status.LeaderTerm = update.LeaderTerm
		status.CommitedIndex = update.CommitedIndex

		for name, replicaUpdate := range update.Replicas {
			if _, ok := f.partitions.Items[id].Replicas[name]; !ok {
				continue
			}
			status := f.partitions.Status[id].Replicas[name]

			status.LastUpdate = replicaUpdate.LastUpdate
			status.LeaderLastContact = replicaUpdate.LeaderLastContact
			status.AppliedIndex = replicaUpdate.AppliedIndex
			status.JoiningStatus = replicaUpdate.JoiningStatus
			status.LeavingStatus = replicaUpdate.LeavingStatus
		}
	}

	f.partitions.StatusVersion++

	return &UpdateResult{
		NewVersion: f.partitions.StatusVersion,
	}, nil
}
