package storage

import (
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ Store = (*store)(nil)
)

// Store defines all possilbe way to interact with the Raft group and its backing FSM storage.
// Read operations are executed directly on the FSM backing storage and the result values will
// be clones of actual stored values. There is no guarantee that the latest version will be
// returned. Write operations wait for the result/error from the FSM except the ones with Asyc
// suffix which perform the write in a fire-and-foreget best effort manner. These should only
// be used for low importance writes that can be retried later without loss of data.
// TODO: implement Store with cache decorator
type Store interface {
	IsEmpty() bool
	ClusterName() string
	PartitionCount() uint32
	Server(id uint64) *Server
	Servers() Servers
	Partitions() Partitions

	Bootstrap(*Bootstrap) (*BootstrapResult, error)
	Register(*Register) (*RegisterResult, error)
	UpdateServers(*UpdateServers) error
	UpdateServersAsync(*UpdateServers) error
}

type store struct {
	raft *multiraft.Raft
	fsm  *FSM
}

func NewStore(raft *multiraft.Raft, fsm *FSM) Store {
	return &store{
		raft: raft,
		fsm:  fsm,
	}
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

func (s *store) ClusterName() string {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()
	return s.fsm.clusterName
}

func (s *store) PartitionCount() uint32 {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()
	return s.fsm.partitionCount
}

func (s *store) IsEmpty() bool {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()
	return s.fsm.clusterName == "" || s.fsm.clusterCreatedTime.IsZero()
}

func (s *store) Server(id uint64) *Server {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()

	server, ok := s.fsm.servers.Items[id]
	if !ok {
		return nil
	}

	return utils.CloneProto(server)
}

func (s *store) Servers() Servers {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()
	return *utils.CloneProto(s.fsm.servers)
}

func (s *store) Partitions() Partitions {
	s.fsm.lock.RLock()
	defer s.fsm.lock.RUnlock()
	return *utils.CloneProto(s.fsm.partitions)
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
