package sessions

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/metrics"
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
				LastSeen:    server.LastSeen,
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
				LastUpdate:   replica.LastUpdate,
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
	lock       sync.Mutex
	servers    map[uint64]*serverStatus
	partitions map[uint32]*partitionStatus
	Unregister func()
}

type serverStatus struct {
	LastSeen    *timestamppb.Timestamp
	LastAddress string
	Dirty       bool
	Labels      metrics.Labels
}

type replicaStatus struct {
	LastUpdate   *timestamppb.Timestamp
	AppliedIndex uint64
	Ready        bool
	Labels       metrics.Labels
}

type partitionStatus struct {
	Id                 uint32
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
		// for now control servers do not create sessions and do not send updates
		if server.Type != control.ServerType_Control {
			sStatus[sid] = newServerStatus(server)
		}
	}

	pStatus := map[uint32]*partitionStatus{}
	for pid, part := range partitions.Items {
		pStatus[pid] = newPartitionStatus(part)
	}

	status := &statusTracker{
		servers:    sStatus,
		partitions: pStatus,
	}

	status.Unregister = metrics.UnregisterAll(
		metrics.RegisterGauge("server.last.update", func(observe metrics.Observe) {
			status.lock.Lock()
			for _, server := range status.servers {
				var diff time.Duration
				if server.LastSeen != nil {
					diff = time.Since(server.LastSeen.AsTime())
				}
				observe(int(diff), server.Labels)
			}
			status.lock.Unlock()
		}),
		metrics.RegisterGauge("replica.last.update", func(observe metrics.Observe) {
			status.lock.Lock()
			for _, part := range status.partitions {
				for _, replica := range part.Replicas {
					var diff time.Duration
					if replica.LastUpdate != nil {
						diff = time.Since(replica.LastUpdate.AsTime())
					}
					observe(int(diff), replica.Labels)
				}
			}
			status.lock.Unlock()
		}),
		metrics.RegisterGauge("replica.applied.index", func(observe metrics.Observe) {
			status.lock.Lock()
			for _, part := range status.partitions {
				for _, replica := range part.Replicas {
					observe(int(replica.AppliedIndex), replica.Labels)
				}
			}
			status.lock.Unlock()
		}),
	)

	return status
}

func newServerStatus(server *control.Server) *serverStatus {
	return &serverStatus{
		LastSeen:    server.LastSeen,
		LastAddress: server.LastAddress,
		Labels:      metrics.NewLabels("server_name", server.Name, "server_id", server.Id),
	}
}

func newPartitionStatus(part *control.Partition) *partitionStatus {
	replicas := map[string]*replicaStatus{}
	for name, replica := range part.Replicas {
		replicas[name] = newReplicaStatus(part.Id, replica)
	}

	return &partitionStatus{
		Id:            part.Id,
		Replicas:      replicas,
		Leader:        part.Leader,
		LeaderTerm:    part.LeaderTerm,
		CommitedIndex: part.CommitedIndex,
	}
}

func newReplicaStatus(pid uint32, replica *control.Partition_Replica) *replicaStatus {
	return &replicaStatus{
		LastUpdate:   replica.LastUpdate,
		AppliedIndex: replica.AppliedIndex,
		Ready:        replica.Ready,
		Labels:       metrics.NewLabels("partition", pid, "replica_name", replica.Name),
	}
}

func (t *statusTracker) recordNewSession(sess *session) {
	status := t.servers[sess.serverID]
	if status == nil {
		logS.Warn("New session status for unknown server.", "server_id", sess.serverID)
		return
	}

	status.LastSeen = timestamppb.New(time.Now().UTC())
	status.LastAddress = sess.serverAddress
	status.Dirty = true
}

func (t *statusTracker) recordSessionReceived(sess *session) {
	status := t.servers[sess.serverID]
	if status == nil {
		logS.Warn("Session received status for unknown server.", "server_id", sess.serverID)
		return
	}

	status.LastSeen = timestamppb.New(time.Now().UTC())
	status.Dirty = true
}

func (t *statusTracker) recordReplicaStatus(updates map[uint32]*control.DataServerStatus_Replica) bool {
	hasLeaderChanges := false

	for id, update := range updates {
		pStatus := t.partitions[id]
		rStatus := pStatus.Replicas[update.Name]

		if rStatus == nil {
			logS.Warn("Session received status for unknown replica.", "replica", update.Name)
			continue
		}

		rStatus.LastUpdate = timestamppb.New(time.Now().UTC())
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
	t.lock.Lock()
	defer t.lock.Unlock()

	for sid := range t.servers {
		if _, ok := newServers.Items[sid]; !ok {
			delete(t.servers, sid)
		}
	}

	for sid, server := range newServers.Items {
		if _, ok := t.servers[sid]; !ok {
			if server.Type != control.ServerType_Control {
				t.servers[sid] = newServerStatus(server)
			}
		}
	}
}

func (t *statusTracker) syncPartitions(newPartitions *control.Partitions) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for pid, part := range newPartitions.Items {
		if status, ok := t.partitions[pid]; !ok {
			t.partitions[pid] = newPartitionStatus(part)
		} else {
			status.syncReplicas(part.Replicas)
		}
	}
}

func (t *partitionStatus) syncReplicas(newReplicas map[string]*control.Partition_Replica) {
	for name := range t.Replicas {
		if _, ok := newReplicas[name]; !ok {
			delete(t.Replicas, name)
		}
	}

	for name, replica := range newReplicas {
		if _, ok := t.Replicas[name]; !ok {
			t.Replicas[name] = newReplicaStatus(t.Id, replica)
		}
	}
}
