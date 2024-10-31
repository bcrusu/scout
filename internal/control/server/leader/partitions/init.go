package partitions

import (
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/utils"
)

// initAssignments uses a simple card dealing strategy to generate
// the initial replica-to-server assignments.
func (a *Assigner) initAssignments() {
	serverIDs := utils.MakeKeySlice(a.store.Servers().DataServers())
	partitionCount := a.store.PartitionCount()
	replicationFactor := a.config.ReplicationFactor

	state := NewState()

	for _, sid := range serverIDs {
		state.AddServer(sid)
	}

	for pid := range partitionCount {
		state.AddPartition(pid)
	}

	idx := 0

	for pid := range partitionCount {
	NEXT_REPLICA:
		for range replicationFactor {
			for range len(serverIDs) {
				idx = (idx + 1) % len(serverIDs)
				sid := serverIDs[idx]

				if !state.Serv[sid].HasReplica(pid) {
					state.AddJoining(sid, pid)
					continue NEXT_REPLICA
				}
			}

			break
		}
	}

	add := make([]*storage.InitAssignments_Add, 0, int(partitionCount)*replicationFactor)

	for pid, part := range state.Part {
		for sid := range part.Joining {
			add = append(add, &storage.InitAssignments_Add{
				PartitionId: pid,
				ServerId:    sid,
				Voter:       true,
			})
		}
	}

	cmd := &storage.InitAssignments{Add: add}

	if _, err := a.store.InitAssignments(cmd); err != nil {
		log.WithError(err).Error("Init assignments failed.")
	}
}
