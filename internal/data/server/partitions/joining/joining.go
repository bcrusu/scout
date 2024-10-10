package joining

import (
	"context"
	"slices"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	_                shared.Replica = (*Joining)(nil)
	debounceInterval                = 100 * time.Millisecond
)

type Joining struct {
	pid          uint32
	localReplica string
	multiraft    *multiraft.MultiRaft
	dataClient   data.ServiceClient
	db           kv.DB
	log          logging.Logger
	getStatusCh  chan chan<- *control.DataServerStatus_Replica
	cancelFunc   context.CancelFunc
}

type candidates struct {
	replicas []replica
	rrNext   int
}

type replica struct {
	replica  *control.DataServerConfig_Replica
	isLeader bool
}

func New(pid uint32, localReplica string, multiraft *multiraft.MultiRaft, dataClient data.ServiceClient, db kv.DB) *Joining {
	return &Joining{
		pid:          pid,
		localReplica: localReplica,
		multiraft:    multiraft,
		dataClient:   dataClient,
		db:           db,
		log:          logging.WithComponent("replica_joining").With("partition", pid, "replica", localReplica),
		getStatusCh:  make(chan chan<- *control.DataServerStatus_Replica),
	}
}

func (p *Joining) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(p.mainLoop)

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (p *Joining) Stop() {
	p.cancelFunc()
}

func (p *Joining) mainLoop(ctx context.Context) {
	dataServerConfigSub := eventbus.SubscribeDebounced[*control.DataServerConfig](ctx, debounceInterval)
	dataServersSub := eventbus.SubscribeDebounced[*control.DataServers](ctx, debounceInterval)
	defer dataServerConfigSub.Unsubscribe()
	defer dataServersSub.Unsubscribe()

	var config *control.DataServerConfig_Partition
	var part *control.DataServers_Partition

	cctx, cancelCtx := context.WithCancel(context.Background())
	fsm := newRestoreFsm(p.pid, cctx, p.localReplica, p.dataClient, p.db)
	var raft *multiraft.Raft

	ensureRaft := func() {
		if raft != nil || config == nil || part == nil {
			return
		}

		r, err := shared.CreateRaft(p.multiraft, config.Name, p.localReplica, fsm)
		if err != nil {
			p.log.WithError(err).Error(ctx, "Failed to create Raft group.")
			return
		}
		raft = r
	}

	updateCandidates := func() {
		if config != nil && part != nil {
			fsm.candidates.Store(p.makeCandidates(config, part))
		}
	}

	for {
		select {
		case x := <-dataServerConfigSub.Items():
			newConfig := x.Partitions[p.pid]
			if config == nil || config.ETag != newConfig.ETag {
				config = newConfig
				ensureRaft()
				updateCandidates()
			}
		case x := <-dataServersSub.Items():
			newPart := x.Partitions[p.pid]
			if part == nil || part.ETag != newPart.ETag {
				part = newPart
				ensureRaft()
				updateCandidates()
			}
		case statusCh := <-p.getStatusCh:
			if raft == nil {
				statusCh <- nil
			}

			x := raft.GetStats()
			statusCh <- &control.DataServerStatus_Replica{
				Name:              p.localReplica,
				IsLeader:          x.IsLeader,
				LeaderTerm:        x.LeaderTerm,
				LeaderLastContact: durationpb.New(x.LeaderLastContact),
				CommitedIndex:     x.CommitedIndex,
				AppliedIndex:      fsm.index.Load(),
				JoiningStatus:     fsm.status.Load(),
			}
		case <-ctx.Done():
			if raft != nil {
				raft.Stop()
			}

			cancelCtx()
			return
		}
	}
}

func (p *Joining) GetService() shared.Service {
	return nil
}

func (p *Joining) GetStatus() *control.DataServerStatus_Replica {
	statusCh := make(chan *control.DataServerStatus_Replica, 1)
	p.getStatusCh <- statusCh
	return <-statusCh
}

func (p *Joining) makeCandidates(config *control.DataServerConfig_Partition, part *control.DataServers_Partition) *candidates {
	var replicas []replica

	for _, r := range config.Replicas {
		switch {
		case r.Name == p.localReplica:
			continue
		case r.State != control.DataServerConfig_Voter && r.State != control.DataServerConfig_NonVoter:
			continue
		}

		replicas = append(replicas, replica{
			replica:  r,
			isLeader: r.ServerId == part.LeaderServerId,
		})
	}

	// add some randomness...
	utils.ShuffleSlice(replicas)

	slices.SortFunc(replicas, func(a, b replica) int {
		switch {
		case a.isLeader != b.isLeader:
			// leader replica has lowest priority
			if a.isLeader {
				return 1
			}
			return -1
		case a.replica.State != b.replica.State:
			// non-voter replicas first
			if a.replica.State == control.DataServerConfig_NonVoter {
				return -1
			}
			return 1
		default:
			return 0
		}
	})

	return &candidates{replicas: replicas}
}

func (c *candidates) nextReplica() replica {
	i := c.rrNext % len(c.replicas)
	c.rrNext++
	return c.replicas[i]
}
