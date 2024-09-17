package storage

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ Store = (*store)(nil)
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

	IsEmpty() bool
	ClusterName() string
	PartitionCount() uint32
	Servers() *Servers
	Partitions() *Partitions

	Bootstrap(*Bootstrap) (*BootstrapResult, error)
	Register(*Register) (*RegisterResult, error)
	UpdateServers(*UpdateServers) error
	UpdateServersAsync(*UpdateServers) error

	SubscribeServers() utils.Subscriber[*Servers]
	SubscribePartitions() utils.Subscriber[*Partitions]
}

type store struct {
	raft       *multiraft.Raft
	fsm        *FSM
	cancelFunc context.CancelFunc
	sPublisher utils.Publisher[*Servers]
	pPublisher utils.Publisher[*Partitions]
	// all below are cached copies of FSM fields
	lock           sync.RWMutex
	clusterName    string
	partitionCount uint32
	servers        *Servers
	partitions     *Partitions
}

func NewStore(raft *multiraft.Raft, fsm *FSM) *store {
	return &store{
		raft:       raft,
		fsm:        fsm,
		sPublisher: utils.NewPubSub[*Servers](1),
		pPublisher: utils.NewPubSub[*Partitions](1),
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
	notifyDebounced := utils.DebounceChan(ctx, s.fsm.notifyCh, 100*time.Millisecond)

	for {
		select {
		case <-notifyDebounced:
			s.lock.Lock()
			s.fsm.lock.RLock()

			s.clusterName = s.fsm.clusterName
			s.partitionCount = s.fsm.partitionCount

			sPublish := false
			if s.servers == nil || s.servers.Version != s.fsm.servers.Version {
				s.servers = utils.CloneProto(s.fsm.servers)
				sPublish = true
			}

			pPublish := false
			if s.partitions == nil || s.partitions.Version != s.fsm.partitions.Version {
				s.partitions = utils.CloneProto(s.fsm.partitions)
				pPublish = true
			}

			s.fsm.lock.RUnlock()

			if sPublish {
				s.sPublisher.PublishAttempt(s.servers)
			}

			if pPublish {
				s.pPublisher.PublishAttempt(s.partitions)
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

func (s *store) IsEmpty() bool {
	return s.ClusterName() == ""
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

func (s *store) UpdateServers(cmd *UpdateServers) error {
	_, err := applyR[*emptyResult](s.raft, cmd)
	return err
}

func (s *store) UpdateServersAsync(cmd *UpdateServers) error {
	return applyAsync(s.raft, cmd)
}

func (s *store) SubscribeServers() utils.Subscriber[*Servers] {
	return s.sPublisher.Subscribe(1)
}

func (s *store) SubscribePartitions() utils.Subscriber[*Partitions] {
	return s.pPublisher.Subscribe(1)
}

func applyR[R any](raft *multiraft.Raft, payload payload) (R, error) {
	var zero R
	cmd, err := newCommand(payload)
	if err != nil {
		return zero, err
	}

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

func applyAsync(raft *multiraft.Raft, payload payload) error {
	cmd, err := newCommand(payload)
	if err != nil {
		return err
	}

	data, err := utils.MarshalProto(cmd)
	if err != nil {
		return err
	}

	return raft.ApplyAsync(data)
}
