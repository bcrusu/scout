package partitions

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/leader"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/durationpb"
)

type replica struct {
	name        string
	config      leader.TxnConfig
	multiraft   *multiraft.MultiRaft
	dataClient  data.ServiceClient
	log         logging.LoggerNoContext
	updateCh    chan *control.DataServerConfig_Partition
	getStatusCh chan getStatusCmd
	serving     atomic.Pointer[replicaServing]
	joining     atomic.Pointer[replicaJoining]
	leaving     atomic.Pointer[replicaLeaving]
	cancelFunc  context.CancelFunc
}

type getStatusCmd struct {
	statusCh chan *control.DataServerStatus_Replica
}

func (p *replica) Start(ctx context.Context, config *control.DataServerConfig_Partition) {
	mainLoop, cancelFunc := utils.WithCancelAndWait(func(ctx context.Context) {
		p.mainLoop(ctx, config)
	})

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
}

func (p *replica) Stop() {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
}

func (p *replica) mainLoop(ctx context.Context, config *control.DataServerConfig_Partition) {
	dataServersSub := eventbus.SubscribeDebounced[*control.DataServers](ctx, debounceInterval)
	defer dataServersSub.Unsubscribe()

	var dataServers *control.DataServers

	var raft *multiraft.Raft
	var store storage.Store
	ensureRaft := func() bool {
		if raft != nil {
			return true
		} else if dataServers == nil {
			return false
		}
		raft, store = p.tryCreateRaft(config, dataServers)
		return raft != nil
	}

	// Valid transitions are:
	//  - created -> joining || serving
	//  - joining -> serving
	//  - serving -> leaving
	//  - joining -> leaving
	updateReplica := func() {
		replica := config.Replicas[p.name]

		switch replica.State {
		case control.DataServerConfig_Joining:
			if p.joining.Load() != nil || !ensureRaft() {
				return
			}
			joining := &replicaJoining{
				id:    config.Id,
				store: store,
				log:   logging.WithComponent("replica_joining").With("id", config.Id, "name", config.Name, "replica", replica.Name),
			}
			joining.Start(ctx)
			p.joining.Store(joining)
		case control.DataServerConfig_NonVoter, control.DataServerConfig_Voter:
			if x := p.joining.Swap(nil); x != nil {
				x.Stop()
			} else if !ensureRaft() {
				return
			}

			raftServers, ok := p.tryGetRaftServers(config, dataServers)
			if !ok {
				return
			}

			if x := p.serving.Load(); x != nil {
				x.updateCh <- updateServersCmd{etag: config.ETag, servers: raftServers}
				return
			}

			serving := &replicaServing{
				id:         config.Id,
				config:     p.config,
				raft:       raft,
				store:      store,
				dataClient: p.dataClient,
				log:        logging.WithComponent("replica_serving").With("id", config.Id, "name", config.Name, "replica", replica.Name),
				updateCh:   make(chan updateServersCmd, 1),
			}
			serving.Start(ctx, config.ETag, raftServers)
			p.serving.Store(serving)
		case control.DataServerConfig_Leaving:
			if x := p.joining.Swap(nil); x != nil {
				x.Stop()
			}
			if x := p.serving.Swap(nil); x != nil {
				x.Stop()
			}
			if raft != nil {
				raft.Stop()
				raft = nil
			}

			// TODO: delete raft group
			leaving := &replicaLeaving{
				id:    config.Id,
				store: store,
				log:   logging.WithComponent("replica_leaving").With("id", config.Id, "name", config.Name, "replica", replica.Name),
			}
			leaving.Start(ctx)
			p.leaving.Store(leaving)
		}
	}

	for {
		select {
		case dataServers = <-dataServersSub.Items():
			updateReplica()
		case newConfig := <-p.updateCh:
			if newConfig.ETag != config.ETag {
				config = newConfig
				updateReplica()
			}
		case cmd := <-p.getStatusCh:
			var status *control.DataServerStatus_Replica

			if p.joining.Load() != nil || p.serving.Load() != nil {
				x := raft.GetStats()
				status = &control.DataServerStatus_Replica{
					Name:              p.name,
					IsLeader:          x.IsLeader,
					LeaderTerm:        x.LeaderTerm,
					LeaderLastContact: durationpb.New(x.LeaderLastContact),
					CommitedIndex:     x.CommitedIndex,
					AppliedIndex:      store.AppliedIndex(),
				}

				if x := p.joining.Load(); x != nil {
					status.DoneJoining = x.IsDone()
				}
			} else if x := p.leaving.Load(); x != nil {
				status = &control.DataServerStatus_Replica{
					Name:        p.name,
					DoneLeaving: x.IsDone(),
				}
			}

			cmd.statusCh <- status
		case <-ctx.Done():
			if x := p.joining.Swap(nil); x != nil {
				x.Stop()
			}
			if x := p.serving.Swap(nil); x != nil {
				x.Stop()
			}
			if x := p.leaving.Swap(nil); x != nil {
				x.Stop()
			}
			if raft != nil {
				raft.Stop()
			}
			return
		}
	}
}

func (p *replica) tryCreateRaft(config *control.DataServerConfig_Partition, dataServers *control.DataServers) (*multiraft.Raft, storage.Store) {
	replica := config.Replicas[p.name]
	fsm := storage.NewFSM(config.Id, nil) // TODO: real KV DB impl.

	groupID := config.Name
	hasState, err := p.multiraft.HasExistingState(groupID)
	if err != nil {
		p.log.WithError(err).Error("Failed to determine Raft group state.")
		return nil, nil
	}

	raft, err := p.multiraft.New(groupID, fsm, raft.ServerID(replica.Name))
	if err != nil {
		p.log.WithError(err).Error("Failed to start Raft.")
		return nil, nil
	}

	store := storage.NewStore(raft, fsm)

	if hasState {
		return raft, store
	}

	servers, ok := p.tryGetRaftServers(config, dataServers)
	if !ok {
		eventbus.TryPublishRefreshDataServers()
		return nil, nil
	}

	p.log.Debug("Bootstrapping Raft group...")

	if err := raft.Bootstrap(servers...); err != nil {
		p.log.WithError(err).Error("Bootstrap Raft group failed.")
		raft.Stop()
		return nil, nil
	} else {
		p.log.Debug("Bootstrap Raft group success.")
	}

	// later, the partition leader will take ownership over the group and
	// update Raft configuration to match the partition config.

	return raft, store
}

// The replica needs to receive two pieces of info:
//   - 1. the partition config: which gives the list of group replicas and their server IDs.
//   - 2. the data servers list: which gives the current address of each data server ID.
//   - if either is missing, the replica cannot be started, and
//   - because these are received independently in separate events, as per design, they
//     could become out of sync resulting in the same situation (e.g. a new server is
//     registered in the cluster and then added to the partition replica list. If the
//     local server receives first the config, containing the new replica, it will need to
//     wait for the data server list before being able to sync).
//   - The events are kept separate because they convey information with different
//     characteristics: data server config is expected to change slowly, while
//     the server address list could change with every data server restart cycle.
func (p *replica) tryGetRaftServers(config *control.DataServerConfig_Partition, dataServers *control.DataServers) ([]raft.Server, bool) {
	servers := make([]raft.Server, 0, len(config.Replicas))

	for _, replica := range config.Replicas {
		var suffrage raft.ServerSuffrage

		switch replica.State {
		case control.DataServerConfig_Joining, control.DataServerConfig_NonVoter:
			suffrage = raft.Nonvoter
		case control.DataServerConfig_Voter:
			suffrage = raft.Voter
		case control.DataServerConfig_Leaving:
			// skip the leaving replica which results in it being removed from Raft configuration by the group leader
			continue
		default:
			panic(fmt.Sprintf("unhandled replica.State %s", replica.State))
		}

		dataServer, ok := dataServers.Servers[replica.ServerId]
		if !ok {
			return nil, false
		}

		servers = append(servers, raft.Server{
			ID:       raft.ServerID(replica.Name),
			Address:  raft.ServerAddress(dataServer.Address),
			Suffrage: suffrage,
		})
	}

	return servers, true
}

func (p *replica) getService() (data.ServiceServer, bool) {
	v := p.serving.Load()
	if v == nil {
		return nil, false
	}
	return v.getService()
}

func (p *replica) getStatus() *control.DataServerStatus_Replica {
	cmd := &getStatusCmd{
		statusCh: make(chan *control.DataServerStatus_Replica, 1),
	}
	return <-cmd.statusCh
}
