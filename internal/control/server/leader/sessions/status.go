package sessions

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (t *Tracker) writeLatestStatus(ctx context.Context, tracker *statusTracker) {
	update := &storage.UpdateStatus{
		Servers:    map[uint64]*storage.UpdateStatus_Server{},
		Partitions: map[uint32]*storage.UpdateStatus_Partition{},
	}

	for id, server := range tracker.servers {
		if server.Dirty {
			update.Servers[id] = &storage.UpdateStatus_Server{
				LastSeen:    timestamppb.New(server.LastSeen),
				LastAddress: server.LastAddress,
			}
		}
	}

	for id, part := range tracker.partitions {
		if !part.Dirty {
			continue
		}

		replicas := map[string]*storage.UpdateStatus_Replica{}
		for name, replica := range part.Replicas {
			replicas[name] = &storage.UpdateStatus_Replica{
				LastUpdate:   timestamppb.New(replica.LastUpdate),
				AppliedIndex: replica.AppliedIndex,
				Ready:        replica.Ready,
			}
		}

		update.Partitions[id] = &storage.UpdateStatus_Partition{
			Replicas:           replicas,
			Leader:             part.Leader,
			LeaderTerm:         part.LeaderTerm,
			LeaderAppliedIndex: part.LeaderAppliedIndex,
			CommitedIndex:      part.CommitedIndex,
		}
	}

	if len(update.Servers) == 0 && len(update.Partitions) == 0 {
		return
	}

	if err := t.store.UpdateStatus(update); err != nil {
		logS.WithContext(ctx).WithError(err).Error("UpdateServerStatus failed")
		return
	}

	for _, server := range tracker.servers {
		server.Dirty = false
	}

	for _, part := range tracker.partitions {
		part.Dirty = false
	}
}

type statusTracker struct {
	servers    map[uint64]*serverStatus
	partitions map[uint32]*partitionStatus
}

type serverStatus struct {
	LastSeen    time.Time
	LastAddress string
	Dirty       bool
}

type replicaStatus struct {
	LastUpdate   time.Time
	AppliedIndex uint64
	Ready        bool
}

type partitionStatus struct {
	Replicas           map[string]*replicaStatus
	Leader             string
	LeaderTerm         uint64
	LeaderAppliedIndex uint64
	CommitedIndex      uint64
	Dirty              bool
}

func newStatusTracker(servers *control.Servers, partitions *control.Partitions) *statusTracker {
	sStatus := map[uint64]*serverStatus{}
	for sid, server := range servers.Items {
		sStatus[sid] = &serverStatus{
			LastSeen:    server.LastSeen.AsTime(),
			LastAddress: server.LastAddress,
		}
	}

	pStatus := map[uint32]*partitionStatus{}
	for pid, part := range partitions.Items {
		rStatus := map[string]*replicaStatus{}
		for name, replica := range part.Replicas {
			rStatus[name] = &replicaStatus{
				LastUpdate:   replica.LastUpdate.AsTime(),
				AppliedIndex: replica.AppliedIndex,
				Ready:        replica.Ready,
			}
		}

		pStatus[pid] = &partitionStatus{
			Replicas:      rStatus,
			Leader:        part.Leader,
			LeaderTerm:    part.LeaderTerm,
			CommitedIndex: part.CommitedIndex,
		}
	}

	return &statusTracker{
		servers:    sStatus,
		partitions: pStatus,
	}
}

func (t *statusTracker) recordNewSession(sess *session) {
	status := t.servers[sess.serverID]
	if status == nil {
		status = &serverStatus{}
		t.servers[sess.serverID] = status
	}

	status.LastSeen = time.Now().UTC()
	status.LastAddress = sess.serverAddress
	status.Dirty = true
}

func (t *statusTracker) recordSessionReceived(sess *session) {
	status := t.servers[sess.serverID]
	if status == nil {
		return
	}

	status.LastSeen = time.Now().UTC()
	status.Dirty = true
}

func (t *statusTracker) recordReplicaStatus(updates map[uint32]*control.DataServerStatus_Replica) bool {
	hasLeaderChanges := false

	for id, update := range updates {
		pStatus := t.partitions[id]
		rStatus := pStatus.Replicas[update.Name]

		if rStatus == nil {
			rStatus = &replicaStatus{}
			pStatus.Replicas[update.Name] = rStatus
		}

		rStatus.LastUpdate = time.Now().UTC()
		rStatus.Ready = update.Ready

		// leaving replicas report AppliedIndex==0, avoid clearing the last value...
		if update.AppliedIndex != 0 {
			rStatus.AppliedIndex = update.AppliedIndex
		}

		leaderChanged := update.IsLeader && update.LeaderTerm > pStatus.LeaderTerm
		if leaderChanged {
			logS.Debug("Partition leader changed.", "partition", id, "old", pStatus.Leader, "new", update.Name, "term", update.LeaderTerm)

			pStatus.Leader = update.Name
			pStatus.LeaderTerm = update.LeaderTerm
			pStatus.LeaderAppliedIndex = update.AppliedIndex
		}

		pStatus.CommitedIndex = max(pStatus.CommitedIndex, update.CommitedIndex)
		pStatus.Dirty = true

		hasLeaderChanges = hasLeaderChanges || leaderChanged
	}

	return hasLeaderChanges
}

func (t *statusTracker) getServerAddress(serverID uint64) string {
	server, ok := t.servers[serverID]
	if !ok {
		return ""
	}
	return server.LastAddress
}

func (t *statusTracker) getPartitionLeader(pid uint32) string {
	return t.partitions[pid].Leader
}

func (t *statusTracker) syncServers(newServers *control.Servers) {
	for sid := range t.servers {
		if _, ok := newServers.Items[sid]; !ok {
			delete(t.servers, sid)
		}
	}

	for sid, server := range newServers.Items {
		if _, ok := t.servers[sid]; !ok {
			t.servers[sid] = &serverStatus{
				LastSeen:    server.LastSeen.AsTime(),
				LastAddress: server.LastAddress,
			}
		}
	}
}

func (t *statusTracker) syncPartitions(newPartitions *control.Partitions) {
	for pid, part := range t.partitions {
		for name := range part.Replicas {
			if _, ok := newPartitions.Items[pid].Replicas[name]; !ok {
				delete(part.Replicas, name)
			}
		}
	}
}
