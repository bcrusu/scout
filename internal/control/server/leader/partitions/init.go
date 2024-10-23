package partitions

import (
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/utils"
)

// initAssignments uses a simple card dealing strategy to generate
// the initial replica-to-server assignments.
func (a *Assigner) initAssignments() {
	servers := a.store.Servers().DataServers()
	partitionCount := a.store.PartitionCount()
	replicationFactor := a.config.ReplicationFactor

	serverIDs := utils.ShuffleSlice(utils.MakeKeySlice(servers))
	idx := 0

	add := make([]*storage.InitAssignments_Add, 0, int(partitionCount)*replicationFactor)

	for pid := range partitionCount {
		for range replicationFactor {
			add = append(add, &storage.InitAssignments_Add{
				PartitionId: pid,
				ServerId:    serverIDs[idx],
				Voter:       true,
			})

			idx = (idx + 1) % len(serverIDs)
		}
	}

	cmd := &storage.InitAssignments{Add: add}

	if _, err := a.store.InitAssignments(cmd); err != nil {
		log.WithError(err).Error("Init assignments failed.")
	}
}
