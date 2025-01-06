package partitions

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_                utils.Lifecycle = (*Controller)(nil)
	logC                             = logging.New("partition_controller")
	debounceInterval                 = 100 * time.Millisecond
)

type Controller struct {
	id         identity.Identity
	db         storage.DB
	multiraft  *multiraft.Multi
	dataClient client.DataClient
	cancelFunc context.CancelFunc
	lock       sync.RWMutex
	replicas   map[uint32]*replica // map[partition_id]*replica
}

func NewController(id identity.Identity, db storage.DB, multiraft *multiraft.Multi, dataClient client.DataClient) *Controller {
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
	c.cancelFunc = utils.RunAsync(ctx, c.mainLoop)
	return nil
}

func (c *Controller) Stop() {
	c.cancelFunc()
}

func (c *Controller) mainLoop(ctx context.Context) {
	dataServerConfigSub := eventbus.SubscribeDebounced[*control.DataServerConfig](ctx, debounceInterval)
	defer dataServerConfigSub.Unsubscribe()

	var config *control.DataServerConfig

	for {
		select {
		case newConfig := <-dataServerConfigSub.Items():
			if config == nil || newConfig.ETag != config.ETag {
				config = newConfig
				c.syncPartitions(ctx, config)
			}
		case <-ctx.Done():
			c.stopPartitions()
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
		var config *control.DataServerConfig_Replica

		if x, ok := dsConfig.Partitions[pid]; ok {
			config = c.getLocalReplicaConfig(x)
		}

		if config == nil || replica.name != config.Name {
			go replica.Stop()
			delete(c.replicas, pid)
		}
	}

	// start/update assigned
	for pid, x := range dsConfig.Partitions {
		// update
		if replica, ok := c.replicas[pid]; ok {
			replica.setConfig(x)
			continue
		}

		config := c.getLocalReplicaConfig(x)
		if config == nil {
			continue
		}

		replica := newReplica(pid, config.Name, c.multiraft, c.dataClient, c.db)
		replica.setConfig(x)

		go replica.Start(ctx)
		c.replicas[pid] = replica
	}
}

func (c *Controller) stopPartitions() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, part := range c.replicas {
		part.Stop()
	}
}

func (c *Controller) getLocalReplicaConfig(config *control.DataServerConfig_Partition) *control.DataServerConfig_Replica {
	for _, replica := range config.Replicas {
		if replica.ServerId == c.id.ServerID {
			return replica
		}
	}
	logC.Warn("Partition replica not found.", "pid", config.Id, "server_id", c.id.ServerID)
	return nil
}

func (c *Controller) GetReplicaStatus() map[uint32]*control.DataServerStatus_Replica {
	c.lock.RLock()
	defer c.lock.RUnlock()

	result := map[uint32]*control.DataServerStatus_Replica{}

	for id, replica := range c.replicas {
		if status := replica.getStatus(); status != nil {
			result[id] = status
		}
	}

	return result
}
