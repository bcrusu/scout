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

type partition struct {
	data.ServiceServer
	id         uint32
	raft       *multiraft.Raft
	fsm        *storage.FSM
	log        logging.Logger
	updateCh   chan updateCmd
	role       atomic.Pointer[partitionDrainer]
	cancelFunc context.CancelFunc
}

type role interface {
	data.ServiceServer
	utils.Lifecycle
}

type updateCmd struct {
	version uint64
	servers []raft.Server
}

func (p *partition) Start(ctx context.Context, version uint64, servers []raft.Server) {
	if err := p.bootstrapRaft(ctx, servers); err != nil {
		p.log.WithError(err).Error(ctx, "Bootstrap Raft group failed.")
		return
	}

	mainLoop, cancelFunc := utils.WithCancelAndWait(func(ctx context.Context) {
		p.mainLoop(ctx, version, servers)
	})

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
}

func (p *partition) Stop() {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
}

func (p *partition) Update(version uint64, servers []raft.Server) {
	p.updateCh <- updateCmd{
		version: version,
		servers: servers,
	}
}

func (p *partition) mainLoop(ctx context.Context, version uint64, servers []raft.Server) {
	isLeader := false
	updatedRaft := false

	for {
		select {
		case isLeader = <-p.raft.GetLeaderChan():
			// Setting to nil will reject new incoming requests with Unavailable error
			// until the new partition leader/follower transition is ready.
			old := p.role.Swap(nil)
			go old.Stop()

			store := storage.NewStore(p.raft, p.fsm)
			var new role

			if isLeader {
				new = leader.New(p.id, store)
			} else {
				new = follower.New(p.id, store)
			}

			drainer := newPartitionDrainer(new)

			if err := drainer.Start(ctx); err != nil {
				p.log.WithError(err).Errorf(ctx, "Failed to start partition %d role %T. Shutting down...", p.id, new)
				utils.LifecycleShutdown(ctx)
				return
			}

			p.role.Store(drainer)

			if isLeader && !updatedRaft {
				updatedRaft = p.updateRaftServers(servers)
			}
		case cmd := <-p.updateCh:
			if cmd.version == version {
				continue
			}
			version = cmd.version
			servers = cmd.servers

			if isLeader {
				updatedRaft = p.updateRaftServers(servers)
			}
		case <-ctx.Done():
			old := p.role.Swap(nil)
			old.Stop()
			p.raft.Stop()
			return
		}
	}
}

func (p *partition) bootstrapRaft(ctx context.Context, newServers []raft.Server) error {
	if oldServers, err := p.raft.GetServers(); err != nil {
		return err
	} else if len(oldServers) == 0 {
		p.log.Debug(ctx, "Bootstrapping Raft group...")

		if err := p.raft.Bootstrap(newServers...); err != nil {
			return err
		} else {
			p.log.Debug(ctx, "Bootstrap Raft group success.")
		}
	} else {
		// later, the partition leader will take ownership over the group and
		// update Raft members to match the partition config.
		p.log.Debug(ctx, "Raft group already bootstrapped.")
	}

	return nil
}

func (p *partition) updateRaftServers(newServers []raft.Server) bool {
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

	// It is paramount that the Raft group does not lose quorum during member
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
			if err == errors.NotLeader {
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
			if err == errors.NotLeader {
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

func (p *partition) getRole() (role, error) {
	v := p.role.Load()
	if v == nil {
		return nil, errors.Unavailable
	}
	return v, nil
}

func (p *partition) Set(ctx context.Context, req *data.SetRequest) (*data.SetResponse, error) {
	if role, err := p.getRole(); err != nil {
		return nil, err
	} else {
		return role.Set(ctx, req)
	}
}

func (p *partition) Get(ctx context.Context, req *data.GetRequest) (*data.GetResponse, error) {
	if role, err := p.getRole(); err != nil {
		return nil, err
	} else {
		return role.Get(ctx, req)
	}
}

func (p *partition) Delete(ctx context.Context, req *data.DeleteRequest) (*data.DeleteResponse, error) {
	if role, err := p.getRole(); err != nil {
		return nil, err
	} else {
		return role.Delete(ctx, req)
	}
}
