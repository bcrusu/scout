package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (f *FSM) applyUpdateAssignments(appendedAt time.Time, cmd *UpdateAssignments) (*UpdateResult, error) {
	if cmd.IfMatch != 0 && cmd.IfMatch != f.partitions.AssignmentsVersion {
		return nil, errors.FailedPrecondition
	} else if !f.partitions.HasAssignments() {
		// init assignments did not happen
		return nil, errors.InvalidRequest
	} else if err := f.validateUpdateAssignments(cmd); err != nil {
		return nil, err
	}

	changed := map[uint32]bool{}

	for _, x := range cmd.Add {
		part := f.partitions.Items[x.PartitionId]
		name := f.nextReplicaName(part.Id)

		part.Replicas[name] = &control.Partition_Replica{
			Name:                name,
			ServerId:            x.ServerId,
			State:               control.ReplicaState_Joining,
			CreatedTime:         timestamppb.New(appendedAt),
			StateTransitionTime: timestamppb.New(appendedAt),
		}
		changed[x.PartitionId] = true
	}

	for _, x := range cmd.Update {
		replica := f.getReplica(x.PartitionId, x.Replica)
		replica.State = x.State
		replica.StateTransitionTime = timestamppb.New(appendedAt)
		changed[x.PartitionId] = true
	}

	for _, x := range cmd.Remove {
		part := f.partitions.Items[x.PartitionId]
		delete(part.Replicas, x.Replica)
		changed[x.PartitionId] = true
	}

	for pid := range changed {
		part := f.partitions.Items[pid]
		part.AssignmentsVersion++
	}

	f.partitions.MaxImbalance = cmd.MaxImbalance
	f.partitions.AssignmentsVersion++
	f.partitions.Version++

	return &UpdateResult{
		NewVersion: f.partitions.AssignmentsVersion,
	}, nil
}

func (f *FSM) validateUpdateAssignments(cmd *UpdateAssignments) error {
	if cmd.IsEmpty() {
		return errors.Error("is empty")
	}

	for _, x := range cmd.Add {
		if !f.isValidPartitionID(x.PartitionId) || !f.isValidDataServer(x.ServerId) {
			return errors.Error("has invalid Add fields")
		}

		// two replicas on the same server would be redundant
		if replica := f.getReplicaByServer(x.PartitionId, x.ServerId); replica != nil {
			return errors.Error("has partition with replicas on the same server")
		}
	}

	for _, x := range cmd.Update {
		if !f.isValidPartitionID(x.PartitionId) || !f.isValidReplicaName(x.PartitionId, x.Replica) {
			return errors.Error("has invalid Update fields")
		}

		if !f.isValidReplicaTransition(x.PartitionId, x.Replica, x.State) {
			return errors.Error("has invalid state transition")
		}

		part := f.partitions.Items[x.PartitionId]
		servingCount := part.ServingReplicaCount()

		// does not allow the last serving replica to leave
		if servingCount == 1 && !x.State.IsServing() {
			return errors.Error("cannot remove last serving replica")
		}
	}

	for _, x := range cmd.Remove {
		if !f.isValidPartitionID(x.PartitionId) || !f.isValidReplicaName(x.PartitionId, x.Replica) {
			return errors.Error("has invalid Remove fields")
		}

		// it first needs to transition to leaving, then it can be removed
		if replica := f.getReplica(x.PartitionId, x.Replica); replica.State != control.ReplicaState_Leaving {
			return errors.Error("cannot remove non-leaving replica")
		}
	}

	return nil
}
