package storage

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
)

var (
	_                Store = (*store)(nil)
	debounceInterval       = 50 * time.Millisecond
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

	Raft() *multiraft.Raft
	Bootstrapped() bool
	ClusterName() string
	PartitionCount() uint32
	Servers() *Servers
	Partitions() *Partitions

	Bootstrap(*Bootstrap) (*BootstrapResult, error)
	Register(*Register) (*RegisterResult, error)
	UpdateStatus(*UpdateStatus) error
	InitAssignments(*InitAssignments) (*UpdateResult, error)
	UpdateAssignments(*UpdateAssignments) (*UpdateResult, error)
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
	s.cancelFunc = utils.RunAsync(ctx, s.mainLoop)
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
			if s.servers == nil || s.servers.Version != s.fsm.servers.Version {
				s.servers = utils.CloneProto(s.fsm.servers)
				publishServers = true
			}

			publishPartitions := false
			if s.partitions == nil || s.partitions.Version != s.fsm.partitions.Version {
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

func (s *store) Raft() *multiraft.Raft {
	return s.raft
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
	result, err := applyR[*RegisterResult](s.raft, cmd)
	if err != nil {
		return nil, err
	}

	if cmd.Type == ServerType_Control {
		// For control plane servers, add them immediately to the Raft group.
		// If the operation fails, the server can retry later with the same token
		// to recover the original id/name pair assigned above by the store in the
		// initial request.
		server := raft.Server{
			ID:       raft.ServerID(result.ServerName),
			Address:  raft.ServerAddress(cmd.Address),
			Suffrage: raft.Voter,
		}

		if err := s.raft.AddOrUpdateServer(server); err != nil {
			return nil, err
		}
	}

	// Gives the subscribers/the session tracker a chance to observe the
	// newly added server before returning to caller. This way the very
	// first new session request is not rejected.
	<-time.After(3 * debounceInterval)
	return result, nil
}

func (s *store) UpdateStatus(cmd *UpdateStatus) error {
	return apply(s.raft, cmd)
}

func (s *store) InitAssignments(cmd *InitAssignments) (*UpdateResult, error) {
	return applyR[*UpdateResult](s.raft, cmd)
}

func (s *store) UpdateAssignments(cmd *UpdateAssignments) (*UpdateResult, error) {
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

func apply(raft *multiraft.Raft, payload payload) error {
	cmd := newCommand(payload)

	data, err := utils.MarshalProto(cmd)
	if err != nil {
		return err
	}

	if _, err := raft.Apply(data); err != nil {
		return err
	}

	return nil
}
