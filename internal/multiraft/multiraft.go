package multiraft

import (
	"context"
	"sync"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

var (
	_   utils.Lifecycle = (*Multi)(nil)
	_   rpc.Service     = (*Multi)(nil)
	log                 = logging.New("multiraft").NoContext()
)

// Multi allows working with multiple Raft instances.
type Multi struct {
	config    Config
	stores    stores
	transport *transport.Manager
	lock      sync.Mutex
	rafts     map[uint32]*Raft
}

// NewInmem returns a new Multi instance with transient state.
func NewInmem(config Config, clusterName, localAddress string) *Multi {
	return newMulti(config, clusterName, localAddress, newInmem())
}

// New returns a new Multi instance with persisted state.
func New(config Config, dataDir string, clusterName, localAddress string) *Multi {
	stores := newPersistent(dataDir, config.SnapshotRetainMax)
	return newMulti(config, clusterName, localAddress, stores)
}

func newMulti(config Config, clusterName, localAddress string, stores stores) *Multi {
	transport := newTransport(config, clusterName, localAddress)

	return &Multi{
		config:    config,
		stores:    stores,
		transport: transport,
		rafts:     map[uint32]*Raft{},
	}
}

func (r *Multi) Start(ctx context.Context) error {
	if err := r.stores.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start raft stores")
	}

	return nil
}

// Stop stops all Raft instances.
func (r *Multi) Stop() {
	for id := range r.rafts {
		r.Shutdown(id)
	}

	r.stores.Stop()

	if err := r.transport.Close(); err != nil {
		log.WithError(err).Error("failed to close raft transport")
	}
}

func (r *Multi) RegisterToServer(server *grpc.Server) {
	r.transport.Register(server)
}

// New creates a new Raft instance.
func (r *Multi) New(id uint32, fsm FSM, localID raft.ServerID) (*Raft, error) {
	if raft, ok := r.rafts[id]; ok {
		return raft, nil
	}

	config := r.config.getRaftConfig()
	config.LocalID = localID

	logs, stable, snap, err := r.stores.New(id)
	if err != nil {
		return nil, err
	}

	fsmAdapter := &fsmAdapter{fsm}
	transport := r.transport.Transport(id)

	raft, err := raft.NewRaft(&config, fsmAdapter, logs, stable, snap, transport)
	if err != nil {
		return nil, err
	}

	r.rafts[id] = &Raft{
		localID:        localID,
		requestTimeout: r.config.RequestTimeout,
		raft:           raft,
	}

	return r.rafts[id], nil
}

// Shutdown stops the Raft instance.
func (r *Multi) Shutdown(id uint32) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	x, ok := r.rafts[id]
	if !ok {
		return nil
	}
	delete(r.rafts, id)

	err := x.raft.Shutdown().Error()
	if err != nil {
		log.WithError(err).Errorf("Failed to shutdown raft instance %d.", id)
	}

	return err
}

// Drop shutsdown and removes all Raft instance data.
func (r *Multi) Drop(id uint32) error {
	if err := r.Shutdown(id); err != nil {
		return err
	}

	return r.stores.Drop(id)
}

func (r *Multi) HasExistingState(id uint32) (bool, error) {
	logs, stable, snap, err := r.stores.New(id)
	if err != nil {
		return false, err
	}

	return raft.HasExistingState(logs, stable, snap)
}
