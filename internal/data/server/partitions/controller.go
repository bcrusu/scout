package partitions

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/events"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_                     utils.Lifecycle = (*Controller)(nil)
	logC                                  = logging.WithComponent("partition_controller").NoContext()
	debounceInterval                      = 100 * time.Millisecond
	publishStatusInterval                 = 20 * time.Second
)

type Controller struct {
	id         identity.Identity
	db         kv.DB
	multiraft  *multiraft.MultiRaft
	dataClient data.ServiceClient
	cancelFunc context.CancelFunc
	lock       sync.RWMutex
	replicas   map[uint32]*replica // map[partition_id]*replica
}

func NewController(id identity.Identity, db kv.DB, multiraft *multiraft.MultiRaft, dataClient data.ServiceClient) *Controller {
	c := &Controller{
		id:        id,
		db:        db,
		multiraft: multiraft,
		replicas:  map[uint32]*replica{},
	}

	c.dataClient = &dataClientLocal{c, dataClient}
	return c
}

func (c *Controller) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(c.mainLoop)

	c.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (c *Controller) Stop() {
	c.cancelFunc()

	c.lock.Lock()
	defer c.lock.Unlock()

	for _, part := range c.replicas {
		part.Stop()
	}
}

func (c *Controller) mainLoop(ctx context.Context) {
	dataServerConfigSub := eventbus.SubscribeDebounced[*control.DataServerConfig](ctx, debounceInterval)
	publishStatusTicker := time.NewTicker(publishStatusInterval)
	defer publishStatusTicker.Stop()
	defer dataServerConfigSub.Unsubscribe()

	var config *control.DataServerConfig

	for {
		select {
		case newConfig := <-dataServerConfigSub.Items():
			if config == nil || newConfig.ETag != config.ETag {
				config = newConfig
				c.syncPartitions(ctx, config)
			}
		case <-publishStatusTicker.C:
			eventbus.TryPublish(c.getPartitionsStatus())
		case <-ctx.Done():
			return
		}
	}
}

func (c *Controller) GetService(id uint32) (shared.Service, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if replica, ok := c.replicas[id]; !ok {
		return nil, false
	} else {
		return replica.getService()
	}
}

func (c *Controller) syncPartitions(ctx context.Context, dsConfig *control.DataServerConfig) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// stop unassigned
	for pid, replica := range c.replicas {
		var replicaConfig *control.DataServerConfig_Replica
		if config, ok := dsConfig.Partitions[pid]; ok {
			replicaConfig = c.getLocalReplicaConfig(config)
		}

		if replicaConfig == nil || replica.name != replicaConfig.Name {
			go replica.Stop()
			delete(c.replicas, pid)
		}
	}

	// start assigned
	for pid, config := range dsConfig.Partitions {
		replicaConfig := c.getLocalReplicaConfig(config)
		if replicaConfig == nil || c.replicas[pid] != nil {
			continue
		}

		replica := newReplica(pid, replicaConfig.Name, c.multiraft, c.dataClient, c.db)

		go replica.Start(ctx, config)
		c.replicas[pid] = replica
	}
}

func (c *Controller) getLocalReplicaConfig(config *control.DataServerConfig_Partition) *control.DataServerConfig_Replica {
	for _, replica := range config.Replicas {
		if replica.ServerId == c.id.ServerID {
			return replica
		}
	}
	logC.Warn("Partition replica not found.", "id", config.Id, "name", config.Name, "server_id", c.id.ServerID)
	return nil
}

func (c *Controller) getPartitionsStatus() events.ReplicaStatus {
	c.lock.RLock()
	defer c.lock.RUnlock()

	result := events.ReplicaStatus{}

	for id, replica := range c.replicas {
		if status := replica.getStatus(); status != nil {
			result[id] = status
		}
	}

	return result
}
