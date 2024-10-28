package sessions

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/convert"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (t *Tracker) writeLatestStatus(ctx context.Context, tracker *statusTracker) {
	t.writeServerStatus(ctx, tracker)
	t.writePartitionStatus(ctx, tracker)
}

func (t *Tracker) writeServerStatus(ctx context.Context, tracker *statusTracker) {
	if len(tracker.serversDirty) == 0 {
		return
	}

	updateServers := &storage.UpdateServerStatus{
		IfMatch: tracker.serversVersion,
		Items:   make(map[uint64]*storage.ServerStatus),
	}

	for id := range tracker.serversDirty {
		updateServers.Items[id] = tracker.servers[id]
	}

	result, err := t.store.UpdateServerStatus(updateServers)
	if err != nil {
		logS.WithError(err).Error(ctx, "UpdateServerStatus failed")
		return
	}

	tracker.serversVersion = result.NewVersion
	clear(tracker.serversDirty)
}

func (t *Tracker) writePartitionStatus(ctx context.Context, tracker *statusTracker) {
	if len(tracker.partitionsDirty) == 0 {
		return
	}

	updatePartitions := &storage.UpdatePartitionStatus{
		IfMatch: tracker.partitionsVersion,
		Items:   map[uint32]*storage.PartitionStatus{},
	}

	for id := range tracker.partitionsDirty {
		updatePartitions.Items[id] = tracker.partitions[id]
	}

	result, err := t.store.UpdatePartitionStatus(updatePartitions)
	if err != nil {
		logS.WithError(err).Error(ctx, "UpdatePartitionStatus failed")
		return
	}

	tracker.partitionsVersion = result.NewVersion
	clear(tracker.partitionsDirty)
}

type statusTracker struct {
	serversVersion    uint64
	servers           map[uint64]*storage.ServerStatus
	serversDirty      map[uint64]bool
	partitionsVersion uint64
	partitions        map[uint32]*storage.PartitionStatus
	partitionsDirty   map[uint32]bool
}

func newStatusTracker(servers *storage.Servers, partitions *storage.Partitions) *statusTracker {
	return &statusTracker{
		serversVersion:    servers.StatusVersion,
		servers:           utils.CloneProtoMap(servers.Status),
		serversDirty:      map[uint64]bool{},
		partitionsVersion: partitions.StatusVersion,
		partitions:        utils.CloneProtoMap(partitions.Status),
		partitionsDirty:   map[uint32]bool{},
	}
}

func (t *statusTracker) recordNewSession(sess *session) {
	serverID := sess.serverID
	status := t.servers[serverID]
	if status == nil {
		status = &storage.ServerStatus{}
		t.servers[serverID] = status
	}

	t.servers[serverID].LastSeen = timestamppb.New(time.Now().UTC())
	t.servers[serverID].LastAddress = sess.serverAddress
	t.serversDirty[serverID] = true
}

func (t *statusTracker) recordSessionReceived(sess *session) {
	serverID := sess.serverID
	status := t.servers[serverID]
	if status == nil {
		status = &storage.ServerStatus{}
		t.servers[serverID] = status
	}

	t.servers[serverID].LastSeen = timestamppb.New(time.Now().UTC())
	t.serversDirty[serverID] = true
}

func (t *statusTracker) recordReplicaStatus(updates map[uint32]*control.DataServerStatus_Replica) bool {
	hasLeaderChanges := false

	for id, update := range updates {
		pStatus := t.partitions[id]

		if pStatus.Replicas == nil {
			pStatus.Replicas = map[string]*storage.PartitionStatus_Replica{}
		}

		rStatus := pStatus.Replicas[update.Name]
		if rStatus == nil {
			rStatus = &storage.PartitionStatus_Replica{}
			pStatus.Replicas[update.Name] = rStatus
		}

		rStatus.LastUpdate = timestamppb.New(time.Now().UTC())
		rStatus.AppliedIndex = update.AppliedIndex
		rStatus.JoiningStatus = convert.ToPartitionJoiningStatus(update.JoiningStatus)
		rStatus.LeavingStatus = convert.ToPartitionLeavingStatus(update.LeavingStatus)

		leaderChanged := update.IsLeader && update.LeaderTerm > pStatus.LeaderTerm
		if leaderChanged {
			pStatus.Leader = update.Name
		}

		pStatus.LeaderTerm = max(pStatus.LeaderTerm, update.LeaderTerm)
		pStatus.CommitedIndex = max(pStatus.CommitedIndex, update.CommitedIndex)

		t.partitionsDirty[id] = true
		hasLeaderChanges = hasLeaderChanges || leaderChanged
	}

	return hasLeaderChanges
}

func (t *statusTracker) getServerLastAddress(serverID uint64) string {
	return t.servers[serverID].LastAddress
}

func (t *statusTracker) updateServers(newServers *storage.Servers) {
	for serverID := range t.servers {
		if _, ok := newServers.Items[serverID]; !ok {
			delete(t.servers, serverID)
			delete(t.serversDirty, serverID)
		}
	}

	for serverID, status := range newServers.Status {
		if _, ok := t.servers[serverID]; !ok {
			t.servers[serverID] = utils.CloneProto(status)
		}
	}
}

func (t *statusTracker) updatePartitions(newPartitions *storage.Partitions) {
	for pid, part := range t.partitions {
		for name := range part.Replicas {
			if _, ok := newPartitions.Items[pid].Replicas[name]; !ok {
				delete(part.Replicas, name)
			}
		}
	}
}
