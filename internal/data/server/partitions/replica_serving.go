package partitions

import (
	"context"
	"sync/atomic"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/follower"
	"github.com/bcrusu/graph/internal/data/server/partitions/leader"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/hashicorp/raft"
)

type replicaServing struct {
	id         uint32
	raft       *multiraft.Raft
	store      storage.Store
	dataClient data.ServiceClient
	log        logging.Logger
	updateCh   chan updateServersCmd
	partition  atomic.Pointer[partitionDrainer]
	cancelFunc context.CancelFunc
}

type ServiceReplica interface {
	data.ServiceServer
	utils.Lifecycle
	IsLeader() bool
}

type updateServersCmd struct {
	etag    string
	servers []raft.Server
}

func (p *replicaServing) Start(ctx context.Context, etag string, servers []raft.Server) {
	mainLoop, cancelFunc := utils.WithCancelAndWait(func(ctx context.Context) {
		p.mainLoop(ctx, etag, servers)
	})

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
}

func (p *replicaServing) Stop() {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
}

func (p *replicaServing) mainLoop(ctx context.Context, etag string, servers []raft.Server) {
	isLeader := false
	updatedRaft := false

	for {
		select {
		case next := <-p.raft.LeaderChan():
			if next == isLeader {
				continue
			}
			isLeader = next

			// Setting to nil will reject new incoming requests with Unavailable error
			// until the new partition leader/follower transition is ready.
			old := p.partition.Swap(nil)
			go old.Stop()

			var new ServiceReplica

			if isLeader {
				new = leader.New(p.id, p.store, p.dataClient)
			} else {
				new = follower.New(p.id, p.store)
			}

			drainer := newPartitionDrainer(new)

			if err := drainer.Start(ctx); err != nil {
				p.log.WithError(err).Errorf(ctx, "Failed to start. Shutting down...", "is_leader", isLeader)
				utils.GracefulShutdown("Failed to start partition.")
				return
			}

			p.partition.Store(drainer)

			if isLeader && !updatedRaft {
				updatedRaft = p.updateRaftServers(servers)
			}
		case cmd := <-p.updateCh:
			if cmd.etag == etag {
				continue
			}
			etag = cmd.etag
			servers = cmd.servers

			if isLeader {
				updatedRaft = p.updateRaftServers(servers)
			}
		case <-ctx.Done():
			old := p.partition.Swap(nil)
			old.Stop()
			p.raft.Stop()
			return
		}
	}
}

func (p *replicaServing) updateRaftServers(newServers []raft.Server) bool {
	oldServers, err := p.raft.GetServers()
	if err != nil {
		p.log.NoContext().WithError(err).Error("Failed to get Raft servers.")
		return false
	}

	findServer := func(list []raft.Server, id raft.ServerID) (raft.Server, bool) {
		for _, x := range list {
			if x.ID == id {
				return x, true
			}
		}
		return raft.Server{}, false
	}

	needsUpdate := func(new, old raft.Server) bool {
		return new.Address != old.Address || new.Suffrage != old.Suffrage
	}

	// It is paramount that the Raft group does not lose quorum during config
	// update which would need manual operator interention for recovery.
	// First, will add/update the new servers. If leader status is lost, return
	// early, else if errors are encountered, skip removing existing servers.
	hasErrors := false
	for _, new := range newServers {
		old, found := findServer(oldServers, new.ID)
		if found && !needsUpdate(new, old) {
			continue
		}

		log := p.log.With("new_id", new.ID, "new_address", new.Address, "new_suffrage", new.Suffrage).NoContext()
		if found {
			log = log.With("old_id", old.ID, "old_address", old.Address, "old_suffrage", old.Suffrage)
		}

		if err := p.raft.AddOrUpdateServer(new); err != nil {
			if errors.Is(err, errors.NotLeader) {
				log.Debug("Raft.AddOrUpdateServer failed. Lost leader status.")
				return false
			} else {
				log.WithError(err).Error("Raft.AddOrUpdateServer failed.")
				hasErrors = true
			}
		} else {
			log.WithError(err).Debug("Raft.AddOrUpdateServer success.")
		}
	}

	if hasErrors {
		return false
	}

	// Next, remove servers only if the above changes were successful. We do not
	// want to remove any server unless the replacements were added.
	for _, old := range oldServers {
		if _, found := findServer(newServers, old.ID); found {
			continue
		}

		log := p.log.With("id", old.ID, "address", old.Address, "suffrage", old.Suffrage).NoContext()

		if err := p.raft.RemoveServer(old.ID); err != nil {
			if errors.Is(err, errors.NotLeader) {
				log.Debug("Raft.RemoveServer failed. Lost leader status.")
				return false
			} else {
				log.WithError(err).Error("Raft.RemoveServer failed.")
			}
		} else {
			log.WithError(err).Debug("Raft.RemoveServer success.")
		}
	}

	return true
}

func (p *replicaServing) getService() (ServiceReplica, bool) {
	v := p.partition.Load()
	if v == nil {
		return nil, false
	}
	return v, true
}
