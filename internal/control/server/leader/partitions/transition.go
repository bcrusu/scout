package partitions

import (
	"math/rand/v2"
	"slices"

	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/utils"
)

type transition struct {
	config config.Partitions
}

func NextState(config config.Partitions, curr *State) *State {
	t := transition{config}
	return t.Next(curr)
}

func (t transition) Next(curr *State) *State {
	next := curr.Clone()

	t.removeLeaving(next)
	t.transitionToServing(next)

	for pid := range rand.Perm(curr.PartitionCount()) {
		t.addMissing(next, uint32(pid))
	}

	for pid := range rand.Perm(curr.PartitionCount()) {
		t.removeExtra(next, uint32(pid))
	}

	t.rebalance(next)

	return next
}

func (transition) transitionToServing(state *State) {
	for _, server := range state.Serv {
		for pid := range server.Joining {
			replica := state.GetReplica(server.Id, pid)

			if replica.Ready {
				state.TransitionJoiningToServing(server.Id, pid)
			}
		}
	}
}

func (transition) removeLeaving(state *State) {
	for _, server := range state.Serv {
		for pid := range server.Leaving {
			replica := state.GetReplica(server.Id, pid)

			if replica.Ready {
				state.RemoveLeaving(server.Id, pid)
			}
		}
	}
}

func (t transition) addMissing(state *State, pid uint32) {
	part := state.Part[pid]

	count := t.config.ReplicationFactor - part.TotalCount()
	count = min(count, t.config.MaxJoiningForPartition-part.JoiningCount())

	if count <= 0 {
		return
	}

	var candidates []uint64
	for _, server := range state.Serv {
		if !server.HasReplica(pid) {
			candidates = append(candidates, server.Id)
		}
	}

	if len(candidates) == 0 {
		return
	}

	slices.SortFunc(candidates, func(i, j uint64) int {
		s1 := state.Serv[i]
		s2 := state.Serv[j]

		// servers with less replicas first
		if x := s1.TotalCount() - s2.TotalCount(); x != 0 {
			return x
		} else {
			// prefer servers with less joining replicas
			return s1.JoiningCount() - s2.JoiningCount()
		}
	})

	for i := 0; i < len(candidates) && count > 0; i++ {
		sid := candidates[i]

		if t.canAddReplica(state, sid, pid) {
			state.AddJoining(sid, pid)
			count--
		}
	}
}

func (t transition) removeExtra(state *State, pid uint32) {
	part := state.Part[pid]

	count := part.ServingCount() - t.config.ReplicationFactor
	if count <= 0 {
		return
	}

	candidates := utils.MakeKeySlice(part.Serving)

	slices.SortFunc(candidates, func(i, j uint64) int {
		s1 := state.Serv[i]
		s2 := state.Serv[j]

		// servers with more replicas first
		if x := s1.TotalCount() - s2.TotalCount(); x != 0 {
			return x
		} else {
			r1 := state.GetReplica(s1.Id, pid)
			r2 := state.GetReplica(s2.Id, pid)
			// older replicas first
			return int(r1.CreatedTime.Sub(r2.CreatedTime))
		}
	})

	for i := 0; i < len(candidates) && count > 0; i++ {
		state.TransitionServingToLeaving(candidates[i], pid)
		count--
	}
}

func (t transition) rebalance(state *State) {
OUTER_LOOP:
	for {
		servers := utils.MakeValueSlice(state.Serv)
		slices.SortFunc(servers, func(s1, s2 ServerState) int {
			// desc sort by total replica count
			return s2.TotalCount() - s1.TotalCount()
		})

		for i, src := range servers {
			for j := len(servers) - 1; j > i; j-- {
				dest := servers[j]
				if diff := src.TotalCount() - dest.TotalCount(); diff <= 1 {
					break
				}

				var candidates []uint32
				for pid := range src.Serving {
					if t.canAddReplica(state, dest.Id, pid) {
						candidates = append(candidates, pid)
					}
				}

				if len(candidates) == 0 {
					continue
				}

				utils.ShuffleSlice(candidates)
				state.AddJoining(dest.Id, candidates[0])
				continue OUTER_LOOP
			}
		}

		return
	}
}

func (t transition) canAddReplica(state *State, sid uint64, pid uint32) bool {
	return state.Joining < t.config.MaxJoining &&
		len(state.Serv[sid].Joining) < t.config.MaxJoiningForServer &&
		len(state.Part[pid].Joining) < t.config.MaxJoiningForPartition &&
		!state.Serv[sid].HasReplica(pid)
}
