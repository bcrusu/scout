package partitions

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/events"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_                     utils.Lifecycle = (*Controller)(nil)
	logC                                  = logging.WithComponent("partition_controller").NoContext()
	debounceInterval                      = 100 * time.Millisecond
	publishStatusInterval                 = 20 * time.Second
)

type Controller struct {
	id         identity.Identity
	multiraft  *multiraft.MultiRaft
	dataClient data.ServiceClient
	cancelFunc context.CancelFunc
	lock       sync.RWMutex
	replicas   map[uint32]*replica // map[partition_id]*replica
}

func NewController(id identity.Identity, multiraft *multiraft.MultiRaft, dataClient data.ServiceClient) *Controller {
	c := &Controller{
		id:        id,
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

func (c *Controller) GetServiceReplica(id uint32) (ServiceReplica, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if part, ok := c.replicas[id]; !ok {
		return nil, false
	} else {
		return part.getService()
	}
}

func (c *Controller) syncPartitions(ctx context.Context, dsConfig *control.DataServerConfig) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// stop unassigned
	for id, part := range c.replicas {
		var replicaConfig *control.DataServerConfig_Replica
		if config, ok := dsConfig.Partitions[id]; ok {
			replicaConfig = c.getLocalReplicaConfig(config)
		}

		if replicaConfig == nil || part.name != replicaConfig.Name {
			go part.Stop()
			delete(c.replicas, id)
		}
	}

	// start/update assigned
	for id, config := range dsConfig.Partitions {
		replicaConfig := c.getLocalReplicaConfig(config)
		if replicaConfig == nil {
			continue
		} else if part, ok := c.replicas[id]; ok {
			// update existing
			part.updateCh <- config
			continue
		}

		replica := &replica{
			name:        replicaConfig.Name,
			multiraft:   c.multiraft,
			dataClient:  c.dataClient,
			log:         logging.WithComponent("partition").With("id", id, "name", config.Name).NoContext(),
			updateCh:    make(chan *control.DataServerConfig_Partition, 1),
			getStatusCh: make(chan getStatusCmd),
		}

		go replica.Start(ctx, config)
		c.replicas[id] = replica
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
