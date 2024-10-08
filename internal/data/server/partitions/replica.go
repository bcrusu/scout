package partitions

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/partitions/joining"
	"github.com/bcrusu/graph/internal/data/server/partitions/leaving"
	"github.com/bcrusu/graph/internal/data/server/partitions/serving"
	"github.com/bcrusu/graph/internal/data/server/partitions/shared"
	"github.com/bcrusu/graph/internal/data/server/storage/kv"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

type replica struct {
	pid        uint32
	name       string
	multiraft  *multiraft.MultiRaft
	dataClient data.ServiceClient
	db         kv.DB
	log        logging.LoggerNoContext
	holder     atomic.Pointer[holder]
	cancelFunc context.CancelFunc
}

type holder struct {
	state    control.DataServerConfig_ReplicaState
	instance shared.Replica
}

func newReplica(pid uint32, name string, multiraft *multiraft.MultiRaft, dataClient data.ServiceClient, db kv.DB) *replica {
	return &replica{
		pid:        pid,
		name:       name,
		multiraft:  multiraft,
		dataClient: dataClient,
		db:         db,
		log:        logging.WithComponent("partition").With("id", pid, "replica", name).NoContext(),
	}
}

func (p *replica) Start(ctx context.Context, config *control.DataServerConfig_Partition) {
	mainLoop, cancelFunc := utils.WithCancelAndWait(p.mainLoop)

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
}

func (p *replica) Stop() {
	p.cancelFunc()
}

func (p *replica) mainLoop(ctx context.Context) {
	dataServerConfigSub := eventbus.SubscribeDebounced[*control.DataServerConfig](ctx, debounceInterval)
	defer dataServerConfigSub.Unsubscribe()

	var config *control.DataServerConfig_Partition

	for {
		select {
		case x := <-dataServerConfigSub.Items():
			newConfig := x.Partitions[p.pid]

			if config != nil && config.ETag == newConfig.ETag {
				continue
			} else if replica := x.GetReplica(p.pid, p.name); replica != nil {
				p.updateReplicaState(ctx, replica.State)
			} else {
				p.updateReplicaState(ctx, control.DataServerConfig_Unknown)
			}

			config = newConfig
		case <-ctx.Done():
			p.updateReplicaState(ctx, control.DataServerConfig_Unknown)
			return
		}
	}
}

// Valid transitions are:
//   - START   -> joining || serving || leaving
//   - joining -> serving || leaving
//   - serving -> leaving
//   - leaving -> STOP.
func (p *replica) updateReplicaState(ctx context.Context, newState control.DataServerConfig_ReplicaState) {
	oldState := control.DataServerConfig_Unknown
	var old shared.Replica
	var new shared.Replica

	if x := p.holder.Load(); x != nil {
		oldState = x.state
		old = x.instance
	}

	switch newState {
	case control.DataServerConfig_Unknown:
		// stop signal
	case control.DataServerConfig_Joining:
		switch oldState {
		case control.DataServerConfig_Unknown:
			new = joining.New(p.pid, p.name, p.multiraft, p.dataClient, p.db)
		case control.DataServerConfig_Joining:
			new = old
		}
	case control.DataServerConfig_NonVoter, control.DataServerConfig_Voter:
		switch oldState {
		case control.DataServerConfig_Unknown, control.DataServerConfig_Joining:
			new = serving.New(p.pid, p.name, p.multiraft, p.dataClient, p.db)
		case control.DataServerConfig_Voter, control.DataServerConfig_NonVoter:
			new = old
		}
	case control.DataServerConfig_Leaving:
		switch oldState {
		case control.DataServerConfig_Leaving:
			new = old
		default:
			new = leaving.New(p.pid, p.name)
		}
	default:
		panic(fmt.Sprintf("unhandled replica state %s", newState))
	}

	if old != nil && old != new {
		p.holder.Store(nil)
		old.Stop()
	}

	if new == nil {
		if newState != control.DataServerConfig_Unknown {
			p.log.Errorf("Replica failed to transition state from %s to %s.", oldState, newState)
		}
		return
	}

	if new != old {
		new.Start(ctx)
	}

	p.holder.Store(&holder{
		state:    newState,
		instance: new,
	})
}

func (p *replica) getService() (shared.Service, bool) {
	m := p.holder.Load()
	if m == nil {
		return nil, false
	} else if s := m.instance.GetService(); s != nil {
		return s, true
	}
	return nil, false
}

func (p *replica) getStatus() *control.DataServerStatus_Replica {
	m := p.holder.Load()
	if m == nil {
		return nil
	}
	return m.instance.GetStatus()
}
