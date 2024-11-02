package partitions

import (
	"math"
	"time"

	"github.com/bcrusu/scout/internal/utils"
)

type State struct {
	Serv    map[uint64]ServerState
	Part    map[uint32]PartitionState
	Replica map[replicaKey]ReplicaState
	Joining int
}

type ServerState struct {
	Id      uint64
	Serving map[uint32]bool
	Joining map[uint32]bool
	Leaving map[uint32]bool
}

type replicaKey struct {
	ServerId    uint64
	PartitionId uint32
}

type ReplicaState struct {
	Pid         uint32
	Name        string
	CreatedTime time.Time
	Ready       bool
}

type PartitionState struct {
	Id      uint32
	Serving map[uint64]bool
	Joining map[uint64]bool
	Leaving map[uint64]bool
}

func NewState() *State {
	return &State{
		Serv:    map[uint64]ServerState{},
		Part:    map[uint32]PartitionState{},
		Replica: map[replicaKey]ReplicaState{},
	}
}

func (s State) AddServer(sid uint64) {
	if _, ok := s.Serv[sid]; !ok {
		s.Serv[sid] = ServerState{
			Id:      sid,
			Serving: map[uint32]bool{},
			Joining: map[uint32]bool{},
			Leaving: map[uint32]bool{},
		}
	}
}

func (s State) AddPartition(pid uint32) {
	if _, ok := s.Part[pid]; !ok {
		s.Part[pid] = PartitionState{
			Id:      pid,
			Joining: map[uint64]bool{},
			Serving: map[uint64]bool{},
			Leaving: map[uint64]bool{},
		}
	}
}

func (s State) Clone() *State {
	result := &State{
		Serv:    map[uint64]ServerState{},
		Part:    map[uint32]PartitionState{},
		Replica: utils.CloneMap(s.Replica),
		Joining: s.Joining,
	}

	for id, s := range s.Serv {
		result.Serv[id] = s.Clone()
	}

	for id, p := range s.Part {
		result.Part[id] = p.Clone()
	}

	return result
}

func (s State) AddServing(sid uint64, pid uint32) {
	s.Serv[sid].Serving[pid] = true
	s.Part[pid].Serving[sid] = true
}

func (s *State) AddJoining(sid uint64, pid uint32) {
	s.Serv[sid].Joining[pid] = true
	s.Part[pid].Joining[sid] = true
	s.Joining++
}

func (s State) AddLeaving(sid uint64, pid uint32) {
	s.Serv[sid].Leaving[pid] = true
	s.Part[pid].Leaving[sid] = true
}

func (s *State) TransitionJoiningToServing(sid uint64, pid uint32) {
	s.Serv[sid].Serving[pid] = true
	s.Part[pid].Serving[sid] = true

	delete(s.Serv[sid].Joining, pid)
	delete(s.Part[pid].Joining, sid)
	s.Joining--
}

func (s State) TransitionServingToLeaving(sid uint64, pid uint32) {
	s.Serv[sid].Leaving[pid] = true
	s.Part[pid].Leaving[sid] = true

	delete(s.Serv[sid].Serving, pid)
	delete(s.Part[pid].Serving, sid)
}

func (s State) RemoveLeaving(sid uint64, pid uint32) {
	delete(s.Serv[sid].Leaving, pid)
	delete(s.Part[pid].Leaving, sid)
}

func (s State) AddReplica(sid uint64, pid uint32, state ReplicaState) {
	key := replicaKey{sid, pid}
	s.Replica[key] = state
}

func (s State) GetReplica(sid uint64, pid uint32) *ReplicaState {
	key := replicaKey{sid, pid}
	result, ok := s.Replica[key]
	if !ok {
		return nil
	}
	return &result
}

func (s State) MaxImbalance() int {
	minCount := math.MaxInt
	maxCount := 0

	for _, server := range s.Serv {
		count := server.TotalCount()
		minCount = min(minCount, count)
		maxCount = max(maxCount, count)
	}

	return maxCount - minCount
}

func (s ServerState) Clone() ServerState {
	return ServerState{
		Id:      s.Id,
		Joining: utils.CloneMap(s.Joining),
		Serving: utils.CloneMap(s.Serving),
		Leaving: utils.CloneMap(s.Leaving),
	}
}

func (s State) ServerCount() int {
	return len(s.Serv)
}

func (s State) PartitionCount() int {
	return len(s.Part)
}

func (s State) HasReplica(sid uint64, pid uint32) bool {
	part := s.Part[pid]
	return s.GetReplica(sid, pid) != nil || part.Joining[sid] || part.Serving[sid] || part.Leaving[sid]
}

func (s ServerState) TotalCount() int {
	// it considers that leaving replicas are alredy removed with
	// just a matter of time until the actual deletion happens.
	return len(s.Joining) + len(s.Serving)
}

func (s ServerState) JoiningCount() int {
	return len(s.Joining)
}

func (s ServerState) ServingCount() int {
	return len(s.Serving)
}

func (s PartitionState) Clone() PartitionState {
	return PartitionState{
		Id:      s.Id,
		Joining: utils.CloneMap(s.Joining),
		Serving: utils.CloneMap(s.Serving),
		Leaving: utils.CloneMap(s.Leaving),
	}
}

func (s PartitionState) TotalCount() int {
	// similar to ServerState.TotalCount:
	return len(s.Joining) + len(s.Serving)
}

func (s PartitionState) JoiningCount() int {
	return len(s.Joining)
}

func (s PartitionState) ServingCount() int {
	return len(s.Serving)
}
