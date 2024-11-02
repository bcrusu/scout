package partitions_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/control/server/leader/partitions"
)

const (
	replicationFactor = 3
)

func FuzzNextState(f *testing.F) {
	config := config.Partitions{
		ReplicationFactor:      replicationFactor,
		MaxJoining:             math.MaxInt,
		MaxJoiningForServer:    math.MaxInt,
		MaxJoiningForPartition: math.MaxInt,
	}

	f.Add(171, 97)

	f.Fuzz(func(t *testing.T, seed1, seed2 int) {
		curr := newRandomState(seed1, seed2)
		next := partitions.NextState(config, curr)

		t.Logf("Imbalance: %d vs. %d\n", curr.MaxImbalance(), next.MaxImbalance())
	})
}

func newRandomState(seed1, seed2 int) *partitions.State {
	const (
		minServers    = 1
		maxServers    = 50
		minPartitions = 1
		maxPartitions = 100
	)

	rand := rand.New(rand.NewPCG(uint64(seed1), uint64(seed2)))
	randInt := func(min, max int) int {
		return min + (rand.Int() % (max - min))
	}

	randReplicaState := func() control.ReplicaState {
		x := rand.IntN(100)
		switch {
		case x < 5:
			return control.ReplicaState_Leaving
		case x < 20:
			return control.ReplicaState_Joining
		case x < 60:
			return control.ReplicaState_NonVoter
		default:
			return control.ReplicaState_Voter
		}
	}

	randBool := func() bool {
		return rand.IntN(100) < 50
	}

	randTime := func() time.Time {
		return time.Unix(rand.Int64(), 0)
	}

	state := partitions.NewState()
	serverCount := randInt(minServers, maxServers)
	partitionCount := randInt(minPartitions, maxPartitions)
	maxReplicas := min(2*replicationFactor, serverCount)

	for sid := range uint64(serverCount) {
		state.AddServer(sid)
	}

	// each partition...
	for pid := range uint32(partitionCount) {
		state.AddPartition(pid)

		replicaCount := randInt(0, maxReplicas)
		servers := rand.Perm(serverCount)

		// has a bunch of replicas...
		for i := range replicaCount {
			// on different servers...
			sid := uint64(servers[i])

			// with random state
			replicaState := randReplicaState()
			ready := true

			switch {
			case replicaState.IsServing():
				state.AddServing(sid, pid)
			case replicaState == control.ReplicaState_Joining:
				ready = randBool()
				state.AddJoining(sid, pid)
			case replicaState == control.ReplicaState_Leaving:
				ready = randBool()
				state.AddLeaving(sid, pid)
			}

			state.AddReplica(sid, pid, partitions.ReplicaState{
				Pid:         pid,
				Name:        fmt.Sprintf("%d_%d", pid, i),
				CreatedTime: randTime(),
				Ready:       ready,
			})
		}
	}

	return state
}
