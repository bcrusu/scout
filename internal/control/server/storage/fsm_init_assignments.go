package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (f *FSM) applyInitAssignments(appendedAt time.Time, cmd *InitAssignments) (*UpdateResult, error) {
	if f.partitions.IsInitialized() {
		// init needs to happen only once right after bootstrap
		return nil, errors.InvalidRequest
	} else if err := f.validateInitAssignments(cmd); err != nil {
		return nil, err
	}

	for _, x := range cmd.Add {
		part := f.partitions.Items[x.PartitionId]
		name := part.nextReplicaName()

		state := ReplicaState_Voter
		if !x.Voter {
			state = ReplicaState_NonVoter
		}

		part.Replicas[name] = &Partition_Replica{
			Name:                name,
			ServerId:            x.ServerId,
			State:               state,
			CreatedTime:         timestamppb.New(appendedAt),
			StateTransitionTime: timestamppb.New(appendedAt),
		}

		part.Version++
	}

	f.partitions.ItemsVersion++

	return &UpdateResult{
		NewVersion: f.partitions.ItemsVersion,
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
			return errors.InvalidRequest
		}

		pair := pair{x.PartitionId, x.ServerId}
		if seenPairs[pair] {
			return errors.InvalidRequest
		}

		seenPairs[pair] = true
		seenPart[x.PartitionId] = true
	}

	if len(seenPart) != int(f.partitionCount) {
		return errors.InvalidRequest
	}

	return nil
}
