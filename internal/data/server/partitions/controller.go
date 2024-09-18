package partitions

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/hashicorp/raft"
)

var (
	_                utils.Lifecycle = (*Controller)(nil)
	logC                             = logging.WithComponent("partition_controller").NoContext()
	debounceInterval                 = 100 * time.Millisecond
)

type Controller struct {
	id         identity.Identity
	multiraft  *multiraft.MultiRaft
	fsm        *storage.FSM
	publisher  Publisher
	cancelFunc context.CancelFunc
	lock       sync.RWMutex
	partitions map[uint32]*partition
}

type addressMap map[uint64]raft.ServerAddress

type Publisher interface {
	SubscribeDataServerConfig() utils.Subscriber[*control.DataServerConfig]
	SubscribeDataServers() utils.Subscriber[*control.DataServers]
}

func NewController(id identity.Identity, multiraft *multiraft.MultiRaft, fsm *storage.FSM, publisher Publisher) *Controller {
	return &Controller{
		id:         id,
		multiraft:  multiraft,
		fsm:        fsm,
		publisher:  publisher,
		partitions: map[uint32]*partition{},
	}
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

	for _, part := range c.partitions {
		part.Stop()
	}
	clear(c.partitions)
}

func (c *Controller) mainLoop(ctx context.Context) {
	configSubscriber := c.publisher.SubscribeDataServerConfig()
	dataServersSubscriber := c.publisher.SubscribeDataServers()
	defer configSubscriber.Unsubscribe()
	defer dataServersSubscriber.Unsubscribe()
	configDebounced := utils.DebounceChan(ctx, configSubscriber.ItemChan(), debounceInterval)
	dataServersDebounced := utils.DebounceChan(ctx, dataServersSubscriber.ItemChan(), debounceInterval)

	var config *control.DataServerConfig
	var addrs addressMap
	var addrsVersion uint64

	syncNow := func() {
		if config != nil && c.syncPartitions(ctx, config, addrs) {
			dataServersSubscriber.NotifyPublisher()
		}
	}

	for {
		select {
		case newConfig := <-configDebounced:
			if newConfig.Version != config.Version {
				config = newConfig
				syncNow()
			}
		case x := <-dataServersDebounced:
			if x.Version != addrsVersion {
				addrs = c.getAddressMap(x)
				addrsVersion = x.Version
				syncNow()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Controller) GetPartition(id uint32) (data.ServiceServer, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if part, ok := c.partitions[id]; !ok {
		return nil, false
	} else {
		return part, true
	}
}

func (c *Controller) syncPartitions(ctx context.Context, dsConfig *control.DataServerConfig, addrs addressMap) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	needsServersUpdate := false

	// stop unassigned and update changed partitions first
	for id, part := range c.partitions {
		config, found := dsConfig.Partitions[id]

		if !found {
			go part.Stop()
			delete(c.partitions, id)
		} else if c.canSyncPartition(config, addrs) {
			servers, _ := c.getRaftServers(config, addrs)
			go part.Update(config.Version, servers)
		} else {
			needsServersUpdate = true
		}
	}

	// create newly assigned
	for _, config := range dsConfig.Partitions {
		if _, ok := c.partitions[config.Id]; ok {
			continue
		} else if !c.canSyncPartition(config, addrs) {
			needsServersUpdate = true
			continue
		}

		servers, localID := c.getRaftServers(config, addrs)

		groupID := config.Name
		raft, err := c.multiraft.New(groupID, localID)
		if err != nil {
			logC.WithError(err).Error("Failed to start Raft.", "partition", config.Id, "group", groupID)
			continue
		}

		part := &partition{
			id:       config.Id,
			raft:     raft,
			fsm:      c.fsm,
			log:      logging.WithComponent("partition").With("id", config.Id, "group", groupID),
			updateCh: make(chan updateCmd),
		}

		go part.Start(ctx, config.Version, servers)
		c.partitions[config.Id] = part
	}

	return needsServersUpdate
}

func (c *Controller) getRaftServers(config *control.DataServerConfig_Partition, addrs addressMap) ([]raft.Server, raft.ServerID) {
	servers := make([]raft.Server, len(config.Members))
	var localID raft.ServerID

	for i, member := range config.Members {
		if member.ServerId == c.id.ServerID {
			localID = raft.ServerID(member.Name)
		}

		servers[i] = raft.Server{
			ID:       raft.ServerID(member.Name),
			Address:  addrs[member.ServerId],
			Suffrage: raft.Voter,
		}

		if !member.Voter {
			servers[i].Suffrage = raft.Nonvoter
		}
	}

	return servers, localID
}

func (c *Controller) getAddressMap(dataServers *control.DataServers) addressMap {
	result := addressMap{}

	for _, ds := range dataServers.Servers {
		result[ds.Id] = raft.ServerAddress(ds.Address)
	}

	return result
}

// The controller needs to receive two pieces of info to be able to sync the partition:
//   - 1. the local data server config: which gives the list of assigned
//     partitions along with the list of group member/server IDs for each partition.
//   - 2. the data servers list: which gives the current address of each data server.
//   - if either is missing, the sync cannot be performed, and
//   - because these are received independently in separate events, as per design, they
//     could become out of sync resulting in the same situation (e.g. a new server is
//     registered in the cluster and then added to the partition members list. If the
//     local server receives first the config, containing the new member, it will need to
//     wait for the data server list before being able to sync).
//   - The events are kept separate because they convey information with different
//     characteristics: data server config is expected to change slowly, while
//     the server address list could change with every data server restart cycle.
func (c *Controller) canSyncPartition(config *control.DataServerConfig_Partition, addrs addressMap) bool {
	if len(addrs) == 0 {
		return false
	}

	found := false
	for _, member := range config.Members {
		if _, ok := addrs[member.ServerId]; !ok {
			return false
		}

		found = found || member.ServerId == c.id.ServerID
	}

	if !found {
		// This signals a bug in the control plane data server config maker
		// with local data server receiving a partition config where it is not
		// part of group members. The partition should have not been included...
		logC.Errorf("Local server not found in config.")
	}

	return found
}
