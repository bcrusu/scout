package storage

import "fmt"

func (s *Servers) ByID(id uint64) *Server {
	return s.Items[id]
}

func (s *Servers) ByType(stype ServerType) map[uint64]*Server {
	result := map[uint64]*Server{}

	for id, s := range s.Items {
		if s.Type == stype {
			result[id] = s
		}
	}

	return result
}

func (s ReplicaState) IsServing() bool {
	return s == ReplicaState_Voter || s == ReplicaState_NonVoter
}

func (p *Partition) nextReplicaName() string {
	p.LastReplicaId++
	return fmt.Sprintf("%s_%d", p.Name, p.LastReplicaId)
}

func (p *Partition) getReplicaForServer(serverID uint64) *Partition_Replica {
	for _, x := range p.Replicas {
		if x.ServerId == serverID {
			return x
		}
	}
	return nil
}

func (p *Partition) getServingReplicaCount() int {
	result := 0
	for _, x := range p.Replicas {
		if x.State.IsServing() {
			result++
		}
	}
	return result
}
