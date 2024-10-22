package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (f *FSM) applyUpdateAssignments(appendedAt time.Time, cmd *UpdateAssignments) (*UpdateResult, error) {
	if cmd.IfMatch != 0 && cmd.IfMatch != f.partitions.ItemsVersion {
		return nil, errors.FailedPrecondition
	} else if f.partitions.ItemsVersion == 1 {
		// init assignments did not happen
		return nil, errors.InvalidRequest
	} else if err := f.validateUpdateAssignments(cmd); err != nil {
		return nil, err
	}

	changed := map[uint32]bool{}

	for _, x := range cmd.Add {
		part := f.getPartition(x.PartitionId)
		name := part.nextReplicaName()

		part.Replicas[name] = &Partition_Replica{
			Name:                name,
			ServerId:            x.ServerId,
			State:               ReplicaState_Joining,
			CreatedTime:         timestamppb.New(appendedAt),
			StateTransitionTime: timestamppb.New(appendedAt),
		}
		changed[x.PartitionId] = true
	}

	for _, x := range cmd.Update {
		replica := f.getReplicaByName(x.PartitionId, x.Replica)
		replica.State = x.State
		replica.StateTransitionTime = timestamppb.New(appendedAt)
		changed[x.PartitionId] = true
	}

	for _, x := range cmd.Remove {
		part := f.getPartition(x.PartitionId)
		delete(part.Replicas, x.Replica)
		changed[x.PartitionId] = true
	}

	for pid := range changed {
		part := f.getPartition(pid)
		part.Version++
	}

	f.partitions.ItemsVersion++

	return &UpdateResult{
		NewVersion: f.partitions.ItemsVersion,
	}, nil
}

func (f *FSM) validateUpdateAssignments(cmd *UpdateAssignments) error {
	for _, x := range cmd.Add {
		if !f.isValidPartitionID(x.PartitionId) || !f.isValidDataServer(x.ServerId) {
			return errors.InvalidRequest
		}

		// two replicas on the same server would be redundant
		if replica := f.getReplicaByServer(x.PartitionId, x.ServerId); replica != nil {
			return errors.InvalidRequest
		}
	}

	for _, x := range cmd.Update {
		if !f.isValidPartitionID(x.PartitionId) || !f.isValidReplicaName(x.PartitionId, x.Replica) ||
			!f.isValidReplicaTransition(x.PartitionId, x.Replica, x.State) {
			return errors.InvalidRequest
		}

		part := f.getPartition(x.PartitionId)
		servingCount := part.getServingReplicaCount()

		// does not allow the last serving replica to leave
		if servingCount == 1 && !x.State.IsServing() {
			return errors.InvalidRequest
		}
	}

	for _, x := range cmd.Remove {
		if !f.isValidPartitionID(x.PartitionId) || !f.isValidReplicaName(x.PartitionId, x.Replica) {
			return errors.InvalidRequest
		}

		// it first needs to transition to leaving, then it can be removed
		if replica := f.getReplicaByName(x.PartitionId, x.Replica); replica.State != ReplicaState_Leaving {
			return errors.InvalidRequest
		}
	}

	return nil
}
