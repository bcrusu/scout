package storage

import (
	"fmt"
)

func (s *Servers) ByID(id uint64) *Server {
	return s.Items[id]
}

func (s *Servers) ControlServers() map[uint64]*Server {
	return s.byType(ServerType_Control)
}

func (s *Servers) DataServers() map[uint64]*Server {
	return s.byType(ServerType_Data)
}

func (s *Servers) ApiServers() map[uint64]*Server {
	return s.byType(ServerType_Api)
}

func (s *Servers) byType(stype ServerType) map[uint64]*Server {
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

func (p *Partitions) IsInitialized() bool {
	return p.AssignmentsVersion > 0
}

func (p *Partition) nextReplicaName() string {
	p.LastReplicaId++
	return fmt.Sprintf("p%d_r%d", p.Id, p.LastReplicaId)
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

func (u *UpdateAssignments) IsEmpty() bool {
	return len(u.Add) == 0 && len(u.Update) == 0 && len(u.Remove) == 0
}
