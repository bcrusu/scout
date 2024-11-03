package partitions

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/partitions/joining"
	"github.com/bcrusu/scout/internal/data/server/partitions/leaving"
	"github.com/bcrusu/scout/internal/data/server/partitions/serving"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

type replica struct {
	pid         uint32
	name        string
	multiraft   *multiraft.Multi
	dataClient  client.DataClient
	db          storage.DB
	log         logging.Logger
	setConfigCh chan *control.DataServerConfig_Partition
	holder      atomic.Pointer[holder]
	cancelFunc  context.CancelFunc
}

type holder struct {
	state    control.ReplicaState
	instance shared.Replica
}

func newReplica(pid uint32, name string, multiraft *multiraft.Multi, dataClient client.DataClient, db storage.DB) *replica {
	return &replica{
		pid:         pid,
		name:        name,
		multiraft:   multiraft,
		dataClient:  dataClient,
		db:          db,
		log:         logging.New("replica").With("partition", pid, "replica", name),
		setConfigCh: make(chan *control.DataServerConfig_Partition, 1),
	}
}

func (p *replica) Start(ctx context.Context) {
	p.cancelFunc = utils.RunAsync(ctx, p.mainLoop)
}

func (p *replica) Stop() {
	p.cancelFunc()
}

func (p *replica) mainLoop(ctx context.Context) {
	var config *control.DataServerConfig_Partition

	for {
		select {
		case newConfig := <-p.setConfigCh:
			if config != nil && config.ETag == newConfig.ETag {
				continue
			}
			config = newConfig

			replica := p.transitionToState(ctx, config.Replicas[p.name].State)
			if replica != nil {
				replica.SetConfig(config)
			}
		case <-ctx.Done():
			p.transitionToState(ctx, control.ReplicaState_Stopped)
			return
		}
	}
}

// Valid transitions are:
//   - START   -> joining || serving || leaving
//   - joining -> serving || leaving
//   - serving -> leaving
//   - leaving -> STOP.
func (p *replica) transitionToState(ctx context.Context, newState control.ReplicaState) shared.Replica {
	oldState := control.ReplicaState_Stopped
	var old shared.Replica
	var new shared.Replica

	if x := p.holder.Load(); x != nil {
		oldState = x.state
		old = x.instance
	}

	if newState == oldState {
		return old
	}

	log := p.log.With("old_state", oldState, "new_state", newState)
	log.Debugf("Replica is transitioning...")

	switch newState {
	case control.ReplicaState_Stopped:
		// stop signal
	case control.ReplicaState_Joining:
		switch oldState {
		case control.ReplicaState_Stopped:
			new = joining.New(p.pid, p.name, p.multiraft, p.dataClient, p.db.KV())
		case control.ReplicaState_Joining:
			new = old
		}
	case control.ReplicaState_NonVoter, control.ReplicaState_Voter:
		switch oldState {
		case control.ReplicaState_Stopped, control.ReplicaState_Joining:
			new = serving.New(p.pid, p.name, p.multiraft, p.dataClient, p.db)
		case control.ReplicaState_Voter, control.ReplicaState_NonVoter:
			new = old
		}
	case control.ReplicaState_Leaving:
		switch oldState {
		case control.ReplicaState_Leaving:
			new = old
		default:
			new = leaving.New(p.pid, p.name, p.multiraft, p.db.KV())
		}
	default:
		panic(fmt.Sprintf("unhandled replica state %s", newState))
	}

	if old != nil && old != new {
		log.Debug("Stopping old state...")

		p.holder.Store(nil)
		old.Stop()
		log.Debug("Stopped old state.")
	}

	if new == nil {
		// sanity check:
		if newState != control.ReplicaState_Stopped {
			p.log.Errorf("Replica failed to transition state from %s to %s.", oldState, newState)
		}
		return nil
	}

	if new != old {
		log.Debug("Starting new state...")

		if err := new.Start(ctx); err != nil {
			log.WithError(err).Error("Failed to start new state.")
			return nil
		}

		log.Debug("Started new state.")
	}

	p.holder.Store(&holder{
		state:    newState,
		instance: new,
	})

	return new
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

func (p *replica) setConfig(config *control.DataServerConfig_Partition) {
	p.setConfigCh <- config
}
