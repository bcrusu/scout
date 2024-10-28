package serving

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/partitions/follower"
	"github.com/bcrusu/scout/internal/data/server/partitions/leader"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
)

var (
	_                shared.Replica = (*Serving)(nil)
	debounceInterval                = 100 * time.Millisecond
)

type Serving struct {
	pid         uint32
	replica     string
	multiraft   *multiraft.Multi
	dataClient  client.DataClient
	db          storage.DB
	log         logging.Logger
	getStatusCh chan chan<- *control.DataServerStatus_Replica
	partition   atomic.Pointer[partitionDrainer]
	cancelFunc  context.CancelFunc
}

func New(pid uint32, replica string, multiraft *multiraft.Multi, dataClient client.DataClient, db storage.DB) *Serving {
	return &Serving{
		pid:         pid,
		replica:     replica,
		multiraft:   multiraft,
		dataClient:  dataClient,
		db:          db,
		log:         logging.New("replica_serving").With("partition", pid, "replica", replica),
		getStatusCh: make(chan chan<- *control.DataServerStatus_Replica),
	}
}

func (p *Serving) Start(ctx context.Context) error {
	p.cancelFunc = utils.RunAsync(ctx, p.mainLoop)
	return nil
}

func (p *Serving) Stop() {
	p.cancelFunc()
}

func (p *Serving) mainLoop(ctx context.Context) {
	dataServerConfigSub := eventbus.SubscribeDebounced[*control.DataServerConfig](ctx, debounceInterval)
	dataServersSub := eventbus.SubscribeDebounced[*control.DataServers](ctx, debounceInterval)
	defer dataServerConfigSub.Unsubscribe()
	defer dataServersSub.Unsubscribe()

	var partConfig *control.DataServerConfig_Partition
	var dataServers *control.DataServers

	txnManager := txn.NewManager(p.pid, p.db.MVCC())
	fsm := storage.NewFSM(p.pid, p.db.KV(), txnManager)
	var raft *multiraft.Raft
	isLeader := false

	updateRaft := func() {
		if servers := shared.TryMakeRaftServerList(partConfig, dataServers); len(servers) == 0 {
			eventbus.TryPublishRefreshDataServers()
			return
		} else if raft == nil {
			if r, err := shared.CreateRaft(p.multiraft, partConfig.Id, p.replica, fsm, servers...); err != nil {
				p.log.WithError(err).Error(ctx, "Failed to create raft instance.")
			} else {
				raft = r
			}
		} else if isLeader {
			p.updateRaftServers(raft, servers)
		}
	}

	for {
		var leaderChan <-chan bool
		if raft != nil {
			leaderChan = raft.LeaderChan()
		}

		select {
		case next := <-leaderChan:
			if next == isLeader {
				continue
			}
			isLeader = next

			// Setting to nil will reject new incoming requests with Unavailable error
			// until the new partition leader/follower transition is ready.
			if old := p.partition.Swap(nil); old != nil {
				go old.Stop()
			}

			var new service
			store := &raftStore{raft: raft}

			if isLeader {
				txnService := txn.NewService(p.pid, store, txnManager, p.db.MVCC(), p.dataClient)
				new = leader.New(p.pid, p.db.KV(), txnService)
			} else {
				txnService := txn.NewServiceNoWatchdog(p.pid, store, txnManager, p.db.MVCC())
				new = follower.New(p.pid, p.db.KV(), txnService)
			}

			drainer := newPartitionDrainer(new, p.log)

			if err := drainer.Start(ctx); err != nil {
				p.log.WithError(err).Errorf(ctx, "Failed to start. Shutting down...", "is_leader", isLeader)
				utils.GracefulShutdown("Failed to start partition.")
				return
			}

			p.partition.Store(drainer)

			if isLeader {
				// TODO: wait store.Appliedindex == raft.CommitedIndex
				updateRaft()
			}
		case x := <-dataServerConfigSub.Items():
			if new := x.Partitions[p.pid]; partConfig == nil || partConfig.ETag != new.ETag {
				partConfig = new
				updateRaft()
			}
		case x := <-dataServersSub.Items():
			if dataServers == nil || dataServers.ETag != x.ETag {
				dataServers = x
				updateRaft()
			}
		case statusCh := <-p.getStatusCh:
			if raft == nil {
				statusCh <- nil
			}

			x := raft.GetStats()
			statusCh <- &control.DataServerStatus_Replica{
				Name:          p.replica,
				IsLeader:      isLeader,
				LeaderTerm:    x.LeaderTerm,
				CommitedIndex: x.CommitedIndex,
				AppliedIndex:  fsm.AppliedIndex(),
			}
		case <-ctx.Done():
			if old := p.partition.Swap(nil); old != nil {
				old.Stop()
			}
			if raft != nil {
				p.multiraft.Shutdown(p.pid)
			}
			return
		}
	}
}

func (p *Serving) updateRaftServers(instance *multiraft.Raft, newServers []raft.Server) {
	oldServers, err := instance.GetServers()
	if err != nil {
		p.log.NoContext().WithError(err).Error("Failed to get Raft servers.")
		return
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

		if err := instance.AddOrUpdateServer(new); err != nil {
			if errors.Is(err, errors.NotLeader) {
				log.Debug("Raft.AddOrUpdateServer failed. Lost leader status.")
				return
			} else {
				log.WithError(err).Error("Raft.AddOrUpdateServer failed.")
				hasErrors = true
			}
		} else {
			log.WithError(err).Debug("Raft.AddOrUpdateServer success.")
		}
	}

	if hasErrors {
		return
	}

	// Next, remove servers only if the above changes were successful. We do not
	// want to remove any server unless the replacements were added.
	for _, old := range oldServers {
		if _, found := findServer(newServers, old.ID); found {
			continue
		}

		log := p.log.With("id", old.ID, "address", old.Address, "suffrage", old.Suffrage).NoContext()

		if err := instance.RemoveServer(old.ID); err != nil {
			if errors.Is(err, errors.NotLeader) {
				log.Debug("Raft.RemoveServer failed. Lost leader status.")
				return
			} else {
				log.WithError(err).Error("Raft.RemoveServer failed.")
			}
		} else {
			log.WithError(err).Debug("Raft.RemoveServer success.")
		}
	}
}

func (p *Serving) GetService() shared.Service {
	v := p.partition.Load()
	if v == nil {
		return nil
	}
	return v
}

func (p *Serving) GetStatus() *control.DataServerStatus_Replica {
	statusCh := make(chan *control.DataServerStatus_Replica, 1)
	p.getStatusCh <- statusCh
	return <-statusCh
}
