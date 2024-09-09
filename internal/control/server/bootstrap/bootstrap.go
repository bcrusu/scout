package bootstrap

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/hashicorp/raft"
)

const (
	serverNamePrefix = "boot_" // will use a dedicated prefix for initial control plane servers.
)

var (
	retryBackoff = &utils.Backoff{
		MinDelay: 5 * time.Second,
		MaxDelay: 20 * time.Second,
	}
	_   utils.Lifecycle = (*Bootstrapper)(nil)
	log                 = logging.WithComponent("control_bootstrap")
)

type Params struct {
	ClusterName  string
	LocalAddress string
	Peers        []string

	valid       bool
	clusterName string
	peers       []string
	ids         []uint64
	names       []string
	localID     uint64
	localName   string
}

// Bootstrapper is used only once, in the beginning of time, when a new baby cluster is born.
type Bootstrapper struct {
	raft     *multiraft.Raft
	store    storage.Store
	params   Params
	cancelFn context.CancelFunc
}

// NewBootstrapper returns a new Bootstrapper.
func NewBootstrapper(raft *multiraft.Raft, store storage.Store, params Params) *Bootstrapper {
	return &Bootstrapper{
		raft:   raft,
		store:  store,
		params: params,
	}
}

// ValidateParams ensures that we have everything required to start the show.
func ValidateParams(p *Params) error {
	if p == nil || p.ClusterName == "" || len(p.ClusterName) > storage.MaxClusterNameLen ||
		p.LocalAddress == "" || len(p.LocalAddress) > storage.MaxAddressLen {
		return errors.InvalidRequest
	}

	if utils.ContainsDuplicates(p.Peers) {
		return errors.Error("peer list contains duplicates.")
	}

	p.clusterName = p.ClusterName
	p.peers = p.Peers
	if !slices.Contains(p.Peers, p.LocalAddress) {
		p.peers = append(p.peers, p.LocalAddress)
	}

	slices.Sort(p.peers)

	p.ids = make([]uint64, len(p.peers))
	p.names = make([]string, len(p.peers))

	for i := range len(p.peers) {
		p.ids[i] = uint64(i + 1)
		p.names[i] = fmt.Sprintf("%s%d", serverNamePrefix, p.ids[i])
	}

	idx := slices.Index(p.peers, p.LocalAddress)
	p.localID = p.ids[idx]
	p.localName = p.names[idx]
	p.valid = true
	return nil
}

// Start performs the initial Raft cluster configuration and then executes the very first
// write operation setting the initial state in the replicated log, thus ensuring that we have
// a good configuration. It will retry until quorum is reached or until context is canceled.
// In case of error, Bootstrap can be called again using exactly the same parameters from the
// original attempt.
func (b *Bootstrapper) Start(ctx context.Context) error {
	p := b.params
	if !p.valid {
		return errors.Error("params were not validated")
	}

	ctx, b.cancelFn = context.WithCancel(ctx)

	log := log.With("cluster", p.ClusterName, "ids", p.ids, "names", p.names, "peers", p.peers)
	log.Debug(ctx, "Bootstrapping the raft cluster...")

	if err := b.bootstrapRaft(p); err != nil {
		log.WithError(err).Debug(ctx, "Bootstrap raft cluster failed.")
		return err
	} else {
		log.WithError(err).Debug(ctx, "Bootstrap raft cluster success.")
	}

	log.Debug(ctx, "Performing initial write...")

	if err := b.performInitialWrite(ctx, p); err != nil {
		log.WithError(err).Debug(ctx, "First write failed.")
		return err
	} else {
		log.WithError(err).Debug(ctx, "First write success.")
	}

	return nil
}

func (b *Bootstrapper) Stop(ctx context.Context) {
	b.cancelFn()
}

func (b *Bootstrapper) bootstrapRaft(p Params) error {
	servers := make([]raft.Server, len(p.names))

	for i, name := range p.names {
		servers[i] = raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(name),
			Address:  raft.ServerAddress(p.peers[i]),
		}
	}

	if err := b.raft.Bootstrap(servers...); err != nil {
		return errors.Wrap(err, "bootstrap raft cluster failed")
	}

	return nil
}

func (b *Bootstrapper) performInitialWrite(ctx context.Context, p Params) error {
	servers := make([]*storage.Bootstrap_Server, len(p.ids))
	for i, id := range p.ids {
		servers[i] = &storage.Bootstrap_Server{
			Id:   id,
			Name: p.names[i],
		}
	}

	cmd := &storage.Bootstrap{
		ClusterName: p.clusterName,
		Servers:     servers,
	}

	return utils.RetryE(ctx, retryBackoff, func() error {
		if !b.raft.IsLeader() {
			log.Info(ctx, "Not leader. Backing off...")
			return errors.NotLeader
		}

		if !b.store.IsEmpty() {
			log.Info(ctx, "Initial write was completed successfully by another server.")
			return nil
		}

		res, err := storage.ApplyR[storage.BootstrapResult](b.raft, cmd)
		if err != nil {
			log.WithError(err).Error(ctx, "Initial write failed. Retrying...")
			return err
		} else if res.Success {
			log.Info(ctx, "Initial write completed successfully.")
		} else {
			// this scenario should never happen, but if it does, we have a bug.
			log.Error(ctx, "Initial write was declined by the FSM.")
		}
		return nil
	})
}

func (p *Params) LocalID() uint64 {
	return p.localID
}

func (p *Params) LocalName() string {
	return p.localName
}
