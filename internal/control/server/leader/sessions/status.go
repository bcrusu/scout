package sessions

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/utils"
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
		status := t.partitions[id]

		replica := status.Replicas[update.Name]
		if replica == nil {
			replica = &storage.PartitionStatus_Replica{}
			status.Replicas[update.Name] = replica
		}

		replica.LastUpdate = timestamppb.New(time.Now().UTC())
		replica.LeaderLastContact = update.LeaderLastContact
		replica.AppliedIndex = update.AppliedIndex
		replica.DoneJoining = update.DoneJoining
		replica.DoneLeaving = update.DoneLeaving

		leaderChanged := update.IsLeader && update.LeaderTerm > status.LeaderTerm
		if leaderChanged {
			status.Leader = update.Name
		}

		status.LeaderTerm = max(status.LeaderTerm, update.LeaderTerm)
		status.CommitedIndex = max(status.CommitedIndex, update.CommitedIndex)

		t.partitionsDirty[id] = true
		hasLeaderChanges = hasLeaderChanges || leaderChanged
	}

	return hasLeaderChanges
}

func (t *statusTracker) getServerLastAddress(serverID uint64) string {
	return t.servers[serverID].LastAddress
}
