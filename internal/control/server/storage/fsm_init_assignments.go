package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (f *FSM) applyInitAssignments(appendedAt time.Time, cmd *InitAssignments) (*UpdateResult, error) {
	if f.partitions.HasAssignments() {
		// init needs to happen only once right after bootstrap
		return nil, errors.InvalidRequest
	} else if err := f.validateInitAssignments(cmd); err != nil {
		return nil, err
	}

	for _, x := range cmd.Add {
		part := f.partitions.Items[x.PartitionId]
		name := f.nextReplicaName(part.Id)

		state := control.ReplicaState_Voter
		if !x.Voter {
			state = control.ReplicaState_NonVoter
		}

		part.Replicas[name] = &control.Partition_Replica{
			Name:                name,
			ServerId:            x.ServerId,
			State:               state,
			CreatedTime:         timestamppb.New(appendedAt),
			StateTransitionTime: timestamppb.New(appendedAt),
		}

		part.AssignmentsVersion = 1
	}

	f.partitions.AssignmentsVersion = 1
	f.partitions.Version = 1

	return &UpdateResult{
		NewVersion: 1,
	}, nil
}

func (f *FSM) validateInitAssignments(cmd *InitAssignments) error {
	type pair struct {
		pid      uint32
		serverID uint64
	}

	seenPairs := map[pair]bool{}
	seenPart := map[uint32]bool{}

	for _, x := range cmd.Add {
		if !f.isValidPartitionID(x.PartitionId) || !f.isValidDataServer(x.ServerId) {
			return errors.Error("has invalid fields")
		}

		pair := pair{x.PartitionId, x.ServerId}
		if seenPairs[pair] {
			return errors.Error("has duplicate entries")
		}

		seenPairs[pair] = true
		seenPart[x.PartitionId] = true
	}

	if len(seenPart) != int(f.partitionCount) {
		return errors.Error("has missing partitions")
	}

	return nil
}
