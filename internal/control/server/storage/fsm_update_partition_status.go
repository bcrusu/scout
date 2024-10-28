package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
)

func (f *FSM) applyUpdatePartitionStatus(_ time.Time, cmd *UpdatePartitionStatus) (*UpdateResult, error) {
	if cmd.IfMatch != 0 && cmd.IfMatch != f.partitions.StatusVersion {
		return nil, errors.FailedPrecondition
	}

	for pid, pUpdate := range cmd.Items {
		pStatus := f.partitions.Status[pid]

		pStatus.Version++
		pStatus.Leader = pUpdate.Leader
		pStatus.LeaderTerm = pUpdate.LeaderTerm
		pStatus.CommitedIndex = pUpdate.CommitedIndex

		for name, rUpdate := range pUpdate.Replicas {
			if _, ok := f.partitions.Items[pid].Replicas[name]; !ok {
				continue
			}

			rStatus := pStatus.Replicas[name]
			if rStatus == nil {
				rStatus = &PartitionStatus_Replica{}
				pStatus.Replicas[name] = rStatus
			}

			rStatus.LastUpdate = rUpdate.LastUpdate
			rStatus.AppliedIndex = rUpdate.AppliedIndex
			rStatus.JoiningStatus = rUpdate.JoiningStatus
			rStatus.LeavingStatus = rUpdate.LeavingStatus
		}
	}

	f.partitions.StatusVersion++

	return &UpdateResult{
		NewVersion: f.partitions.StatusVersion,
	}, nil
}
