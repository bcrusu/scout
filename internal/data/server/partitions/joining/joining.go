package joining

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/shared"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
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
	db           storage.DB
	log          logging.Logger
	getStatusCh  chan chan<- *control.DataServerStatus_Replica
	cancelFunc   context.CancelFunc
}

func New(pid uint32, localReplica string, multiraft *multiraft.MultiRaft, dataClient data.ServiceClient, db storage.DB) *Joining {
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

	var partConfig *control.DataServerConfig_Partition
	var dataServers *control.DataServers
	var raft *multiraft.Raft
	var fsm *restoreFsm

	createRaft := func() {
		if raft != nil {
			return
		} else if servers := shared.TryMakeRaftServerList(partConfig, dataServers); len(servers) == 0 {
			eventbus.TryPublishRefreshDataServers()
			return
		} else if raft == nil {
			fsm = newRestoreFsm(p.pid, p.log.NoContext(), p.dataClient, p.db)

			if r, err := shared.CreateRaft(p.multiraft, partConfig.Name, p.localReplica, fsm, servers); err != nil {
				p.log.WithError(err).Error(ctx, "Failed to create Raft group.")
			} else {
				raft = r
			}
		}
	}

	for {
		select {
		case x := <-dataServerConfigSub.Items():
			partConfig = x.Partitions[p.pid]
			createRaft()
		case dataServers = <-dataServersSub.Items():
			createRaft()
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
				DoneJoining:       false,
			}
		case <-ctx.Done():
			if raft != nil {
				raft.Stop()
			}
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
