package storage

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_                Store = (*store)(nil)
	debounceInterval       = 100 * time.Millisecond
)

// Store defines all possilbe way to interact with the Raft group and its backing FSM storage.
//
// Read operations are executed directly on the FSM backing storage and the result values will
// be clones of actual stored values. There is no guarantee that the latest version will be
// returned as there could be commited log entries that are yet to be applied to the local FSM.
// For stronger consistency requiremets, should make use of Raft barrier, wait for it to be
// applied, and only then read the FSM state.
//
// Write operations wait for the result/error from the FSM except the ones with Asyc
// suffix which perform the write in a fire-and-foreget best effort manner. These should only
// be used for low importance writes that can be retried later without loss of data.
//
// Subscribers will always observe latest applied FSM state at publish time. The Store does not
// guarantee that all intermediary states since the previous observatiton will be published to
// subscribers as the events are debounced.
type Store interface {
	utils.Lifecycle

	Bootstrapped() bool
	ClusterName() string
	PartitionCount() uint32
	Servers() *Servers
	Partitions() *Partitions

	Bootstrap(*Bootstrap) (*BootstrapResult, error)
	Register(*Register) (*RegisterResult, error)
	UpdateServerStatus(*UpdateServerStatus) (*UpdateResult, error)
	UpdatePartitionStatus(*UpdatePartitionStatus) (*UpdateResult, error)
}

type store struct {
	raft       *multiraft.Raft
	fsm        *FSM
	cancelFunc context.CancelFunc
	// all below are cached copies of FSM fields
	lock           sync.RWMutex
	clusterName    string
	partitionCount uint32
	servers        *Servers
	partitions     *Partitions
}

func NewStore(raft *multiraft.Raft, fsm *FSM) *store {
	return &store{
		raft: raft,
		fsm:  fsm,
	}
}

func (s *store) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(s.mainLoop)

	s.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (s *store) Stop() {
	s.cancelFunc()
}

func (s *store) mainLoop(ctx context.Context) {
	notifyDebounced := utils.DebounceChan(ctx, s.fsm.notifyCh, debounceInterval)

	for {
		select {
		case <-notifyDebounced:
			s.lock.Lock()
			s.fsm.lock.RLock()

			s.clusterName = s.fsm.clusterName
			s.partitionCount = s.fsm.partitionCount

			publishServers := false
			if s.servers == nil || s.servers.ItemsVersion != s.fsm.servers.ItemsVersion ||
				s.servers.StatusVersion != s.fsm.servers.StatusVersion {
				s.servers = utils.CloneProto(s.fsm.servers)
				publishServers = true
			}

			publishPartitions := false
			if s.partitions == nil || s.partitions.ItemsVersion != s.fsm.partitions.ItemsVersion ||
				s.partitions.StatusVersion != s.fsm.partitions.StatusVersion {
				s.partitions = utils.CloneProto(s.fsm.partitions)
				publishPartitions = true
			}

			s.fsm.lock.RUnlock()

			if publishServers {
				eventbus.TryPublish(s.servers)
			}

			if publishPartitions {
				eventbus.TryPublish(s.partitions)
			}

			s.lock.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (s *store) ClusterName() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.clusterName
}

func (s *store) PartitionCount() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.partitionCount
}

func (s *store) Bootstrapped() bool {
	return s.ClusterName() != ""
}

func (s *store) Servers() *Servers {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.servers
}

func (s *store) Partitions() *Partitions {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.partitions
}

func (s *store) Bootstrap(cmd *Bootstrap) (*BootstrapResult, error) {
	return applyR[*BootstrapResult](s.raft, cmd)
}

func (s *store) Register(cmd *Register) (*RegisterResult, error) {
	return applyR[*RegisterResult](s.raft, cmd)
}

func (s *store) UpdateServerStatus(cmd *UpdateServerStatus) (*UpdateResult, error) {
	return applyR[*UpdateResult](s.raft, cmd)
}

func (s *store) UpdatePartitionStatus(cmd *UpdatePartitionStatus) (*UpdateResult, error) {
	return applyR[*UpdateResult](s.raft, cmd)
}

func applyR[R any](raft *multiraft.Raft, payload payload) (R, error) {
	var zero R
	cmd := newCommand(payload)

	data, err := utils.MarshalProto(cmd)
	if err != nil {
		return zero, err
	}

	result, err := raft.Apply(data)
	if err != nil {
		return zero, err
	}

	if t, ok := result.(R); !ok {
		return zero, errors.Errorf("bad result type from apply: expected %T, got %T", zero, result)
	} else {
		return t, nil
	}
}
