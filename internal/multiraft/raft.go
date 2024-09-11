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
	config     Config
	raft       *raft.Raft
	leaderChan chan bool
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

// NewRaft returns a new Raft group.
func NewRaft(config Config) *Raft {
	return &Raft{
		config:     config,
		leaderChan: make(chan bool),
	}
}

// Start starts the Raft group.
func (r *Raft) Start(ctx context.Context) error {
	logStore := NewLogStore()
	stableStore := NewStableStore()
	snapshotStore := NewSnapshotStore()

	cfg := r.config.getRaftConfig()
	cfg.NotifyCh = r.leaderChan

	fsm := &fsmAdapter{r.config.FSM}
	raft, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapshotStore, r.config.Transport)
	if err != nil {
		return errors.Wrap(err, "failed to create Raft")
	}

	r.raft = raft
	return nil
}

// Stop stops the Raft group.
func (r *Raft) Stop(ctx context.Context) {
	if err := r.raft.Shutdown().Error(); err != nil {
		log.WithContext().WithError(err).Warn(ctx, "Shutdown call returned error")
	}
}

// Bootstrap is called only once, in the beggining, when the cluster is created.
// It configures the initial list of servers which must also include the local server.
func (r *Raft) Bootstrap(initialServers ...raft.Server) error {
	sid := raft.ServerID(r.config.ID)
	saddress := raft.ServerAddress(r.config.Address)

	if _, ok := findServerForIdAndAddress(initialServers, sid, saddress); !ok {
		return errors.Error("initial server list does not contain the local server")
	}

	cfg := raft.Configuration{Servers: initialServers}

	err := r.raft.BootstrapCluster(cfg).Error()
	if err != nil && err != raft.ErrCantBootstrap {
		return errors.Wrap(err, "Raft bootstrap failed")
	}

	return nil
}

// GetConfiguration returns latest configuration.
func (r *Raft) GetConfiguration() (raft.Configuration, error) {
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, errors.Wrap(err, "failed to get Raft configuration")
	}

	return future.Configuration(), nil
}

// IsLeader returns true if the node is currently the leader.
func (r *Raft) IsLeader() bool {
	return r.raft.State() == raft.Leader
}

// GetLeader returns current leader identifier and address if available.
func (r *Raft) GetLeader() (string, string, error) {
	addr, id := r.raft.LeaderWithID()
	if id == "" {
		return "", "", errors.UnknownLeader
	}
	return string(id), string(addr), nil
}

// GetLeaderChan returns a chan used to observe leadership changes for current server.
func (r *Raft) GetLeaderChan() <-chan bool {
	return r.leaderChan
}

// AddVoter adds the provided server to the running cluster.
// Must be invoked on the leader.
func (r *Raft) AddVoter(ctx context.Context, id, address string) error {
	if err := r.checkLeader(); err != nil {
		return err
	}

	log := log.With("id", id, "address", address).WithContext()
	cfg, err := r.GetConfiguration()
	if err != nil {
		return err
	}

	sid := raft.ServerID(id)
	saddress := raft.ServerAddress(address)

	if server, ok := findServerForId(cfg.Servers, sid); ok {
		if server.Address == saddress {
			log.Info(ctx, "Server is already in cluster. Nothing to do.")
			return nil
		} else {
			log.Info(ctx, "Server is already in cluster, but with a different address. Will update its address.", "old_address", server.Address)
		}
	} else {
		log.Info(ctx, "Adding server to cluster...")
	}

	if err := r.raft.AddVoter(sid, saddress, 0, r.config.RequestTimeout).Error(); err != nil {
		log.WithError(err).Error(ctx, "Add server to cluster failed.")
		return r.convertError(err)
	}

	return nil
}

// RemoveServer removes the server from the cluster.
// Must be invoked on the leader.
func (r *Raft) RemoveServer(ctx context.Context, id string) error {
	if err := r.checkLeader(); err != nil {
		return err
	}

	sid := raft.ServerID(id)

	if err := r.raft.RemoveServer(sid, 0, r.config.RequestTimeout).Error(); err != nil {
		log.WithContext().WithError(err).Error(ctx, "Remove server from cluster failed.")
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
