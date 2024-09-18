package multiraft

import (
	"context"
	"io"
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/hashicorp/raft"
)

var (
	_ utils.Lifecycle  = (*Raft)(nil)
	_ raft.FSM         = (*fsmAdapter)(nil)
	_ raft.FSMSnapshot = (*fsmSnapshot)(nil)
)

// Raft represents a single Raft group.
type Raft struct {
	localID        raft.ServerID
	bindAddress    raft.ServerAddress
	requestTimeout time.Duration
	raft           *raft.Raft
	leaderChan     chan bool
}

type FSM interface {
	Apply(index uint64, appendedAt time.Time, data []byte) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

type fsmAdapter struct {
	fsm FSM
}

type fsmSnapshot struct {
	data []byte
}

func (r *Raft) Start(ctx context.Context) error {
	return nil
}

// Stop stops the Raft group.
func (r *Raft) Stop() {
	if err := r.raft.Shutdown().Error(); err != nil {
		log.WithError(err).Warn("Shutdown call returned error")
	}
}

// Bootstrap is called only once, in the beggining, when the cluster is created.
// It configures the initial list of servers which must also include the local server.
func (r *Raft) Bootstrap(initialServers ...raft.Server) error {
	if _, ok := findServerForIdAndAddress(initialServers, r.localID, r.bindAddress); !ok {
		return errors.Error("initial server list does not contain the local server")
	}

	cfg := raft.Configuration{Servers: initialServers}

	err := r.raft.BootstrapCluster(cfg).Error()
	if err != nil && err != raft.ErrCantBootstrap {
		return errors.Wrap(err, "Raft bootstrap failed")
	}

	return nil
}

// GetServers returns the current configuration server list.
func (r *Raft) GetServers() ([]raft.Server, error) {
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, errors.Wrap(err, "failed to get Raft configuration")
	}

	config := future.Configuration()
	return config.Servers, nil
}

// IsLeader returns true if the node is currently the leader.
func (r *Raft) IsLeader() bool {
	return r.raft.State() == raft.Leader
}

// GetLeader returns current leader identifier and address if available.
func (r *Raft) GetLeader() (raft.ServerID, raft.ServerAddress, bool) {
	addr, id := r.raft.LeaderWithID()
	if id == "" {
		return "", "", false
	}
	return id, addr, true
}

// GetLeaderChan returns a chan used to observe leadership changes for current server.
func (r *Raft) GetLeaderChan() <-chan bool {
	return r.leaderChan
}

// AddOrUpdateServer adds the provided server to the running cluster. If already part of
// the cluster, its address and suffrage will be updated.
// Must be invoked on the leader.
func (r *Raft) AddOrUpdateServer(new raft.Server) error {
	if err := r.checkLeader(); err != nil {
		return err
	}

	log := log.With("id", new.ID, "address", new.Address, "suffrage", new.Suffrage)

	servers, err := r.GetServers()
	if err != nil {
		return err
	}

	needsDemote := false
	if old, found := findServerForId(servers, new.ID); found {
		needsDemote = old.Suffrage == raft.Voter && new.Suffrage == raft.Nonvoter
	}

	var addFunc func(raft.ServerID, raft.ServerAddress, uint64, time.Duration) raft.IndexFuture
	if new.Suffrage == raft.Voter {
		addFunc = r.raft.AddVoter
	} else {
		addFunc = r.raft.AddNonvoter
	}

	if err := addFunc(new.ID, new.Address, 0, r.requestTimeout).Error(); err != nil {
		log.WithError(err).Error("Add server failed.")
		return r.convertError(err)
	} else {
		log.Info("Add server success.")
	}

	if !needsDemote {
		return nil
	}

	if err := r.raft.DemoteVoter(new.ID, 0, r.requestTimeout).Error(); err != nil {
		log.WithError(err).Error("Demote server failed.")
		return r.convertError(err)
	} else {
		log.Info("Demote server success.")
	}

	return nil
}

// RemoveServer removes the server from the cluster.
// Must be invoked on the leader.
func (r *Raft) RemoveServer(id raft.ServerID) error {
	if err := r.checkLeader(); err != nil {
		return err
	}

	if err := r.raft.RemoveServer(id, 0, r.requestTimeout).Error(); err != nil {
		log.WithError(err).Error("Remove server from cluster failed.")
		return r.convertError(err)
	}

	return nil
}

// Apply is used to apply a command to the FSM and waits for the result.
// Must be invoked on the leader.
func (r *Raft) Apply(cmd []byte) (any, error) {
	if err := r.checkLeader(); err != nil {
		return nil, err
	}

	future := r.raft.Apply(cmd, 0)
	if err := future.Error(); err != nil {
		return nil, r.convertError(err)
	}

	resp := future.Response()
	if err, ok := resp.(error); ok {
		return nil, err
	}

	return resp, nil
}

// ApplyAsync is used to apply a command to the FSM and do not wait for the result.
// Must be invoked on the leader.
func (r *Raft) ApplyAsync(cmd []byte) error {
	if err := r.checkLeader(); err != nil {
		return err
	}

	r.raft.Apply(cmd, 0)
	return nil
}

func (r *Raft) checkLeader() error {
	if r.raft.State() != raft.Leader {
		return errors.NotLeader
	}
	return nil
}

func (r *Raft) convertError(err error) error {
	switch err {
	case raft.ErrLeadershipLost, raft.ErrNotLeader, raft.ErrLeadershipTransferInProgress:
		return errors.NotLeader
	}

	return err
}

func (f *fsmAdapter) Apply(log *raft.Log) any {
	if log.Type != raft.LogCommand {
		return nil
	}
	return f.fsm.Apply(log.Index, log.AppendedAt.UTC(), log.Data)
}

func (f *fsmAdapter) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.fsm.Snapshot()
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, raft.ErrNothingNewToSnapshot
	}

	return &fsmSnapshot{data}, nil
}

func (f *fsmAdapter) Restore(reader io.ReadCloser) error {
	defer func() {
		if err := reader.Close(); err != nil {
			log.WithError(err).Error("Snapshot reader failed to close")
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "snapshot read failed")
	}

	return f.fsm.Restore(data)
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if _, err := sink.Write(f.data); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
