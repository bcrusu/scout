package bootstrap

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
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
	ClusterName    string
	LocalAddress   string
	Peers          []string
	PartitionCount uint32

	valid       bool
	serverIDs   []uint64
	serverNames []string
	serverAddrs []string
	identity    identity.Identity
}

// Bootstrapper is used only once, in the beginning of time, when a new baby cluster is born.
type Bootstrapper struct {
	raft       *multiraft.Raft
	store      storage.Store
	idStore    identity.IdentityStore
	params     Params
	cancelFunc context.CancelFunc
}

// NewBootstrapper returns a new Bootstrapper.
func NewBootstrapper(raft *multiraft.Raft, store storage.Store, idStore identity.IdentityStore, params Params) *Bootstrapper {
	return &Bootstrapper{
		raft:    raft,
		store:   store,
		idStore: idStore,
		params:  params,
	}
}

// ValidateParams ensures that we have everything required to start the show.
func ValidateParams(p *Params) error {
	if p == nil || !storage.IsValidClusterName(p.ClusterName) || storage.IsValidAddress(p.LocalAddress) ||
		!storage.IsValidPartitionCount(p.PartitionCount) {
		return errors.InvalidRequest
	}

	if utils.ContainsDuplicates(p.Peers) {
		return errors.Error("peer list contains duplicates.")
	}

	p.serverAddrs = slices.Clone(p.Peers)
	if !slices.Contains(p.serverAddrs, p.LocalAddress) {
		p.serverAddrs = append(p.serverAddrs, p.LocalAddress)
	}

	slices.Sort(p.serverAddrs)

	p.serverIDs = make([]uint64, len(p.serverAddrs))
	p.serverNames = make([]string, len(p.serverAddrs))

	for i := range len(p.serverAddrs) {
		p.serverIDs[i] = uint64(i + 1)
		p.serverNames[i] = fmt.Sprintf("%s%d", serverNamePrefix, p.serverIDs[i])
	}

	idx := slices.Index(p.serverAddrs, p.LocalAddress)
	p.identity = identity.Identity{
		ClusterName: p.ClusterName,
		ServerID:    p.serverIDs[idx],
		ServerName:  p.serverNames[idx],
	}

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

	log := log.With("cluster", p.ClusterName, "ids", p.serverIDs, "names", p.serverNames, "addresses", p.serverAddrs)
	log.Debug(ctx, "Bootstrapping the raft cluster...")

	if err := b.bootstrapRaft(p); err != nil {
		log.WithError(err).Debug(ctx, "Bootstrap raft cluster failed.")
		return err
	} else {
		log.WithError(err).Debug(ctx, "Bootstrap raft cluster success.")
	}

	initalWrite, cancelFunc := utils.WithCancelAndWait(func(ctx context.Context) {
		log.Debug(ctx, "Performing initial write...")

		if err := b.initalWriteWithRetry(ctx, p); err != nil {
			log.WithError(err).Debug(ctx, "Initial write failed.")
		} else {
			log.WithError(err).Debug(ctx, "Initial write success.")
		}

		log.Debug(ctx, "Storing identity...")

		if err := b.storeIdentityWithRetry(ctx, p); err != nil {
			log.WithError(err).Debug(ctx, "Storing identity failed.")
		} else {
			log.WithError(err).Debug(ctx, "Storing identity success.")
		}
	})

	b.cancelFunc = cancelFunc
	go initalWrite(ctx)
	return nil
}

func (b *Bootstrapper) Stop() {
	b.cancelFunc()
}

func (b *Bootstrapper) bootstrapRaft(p Params) error {
	servers := make([]raft.Server, len(p.serverNames))

	for i, name := range p.serverNames {
		servers[i] = raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(name),
			Address:  raft.ServerAddress(p.serverAddrs[i]),
		}
	}

	if err := b.raft.Bootstrap(servers...); err != nil {
		return errors.Wrap(err, "bootstrap raft cluster failed")
	}

	return nil
}

func (b *Bootstrapper) initalWriteWithRetry(ctx context.Context, p Params) error {
	servers := make([]*storage.Bootstrap_Server, len(p.serverIDs))
	for i, id := range p.serverIDs {
		servers[i] = &storage.Bootstrap_Server{
			Id:      id,
			Name:    p.serverNames[i],
			Address: p.serverAddrs[i],
		}
	}

	cmd := &storage.Bootstrap{
		ClusterName:    p.ClusterName,
		Servers:        servers,
		PartitionCount: p.PartitionCount,
	}

	return utils.RetryE(ctx, retryBackoff, func() error {
		if !b.raft.IsLeader() {
			log.Info(ctx, "Not leader. Backing off...")
			return errors.NotLeader
		}

		if clusterName := b.store.ClusterName(); clusterName == p.ClusterName {
			log.Info(ctx, "Initial write was completed successfully by another server.")
			return nil
		} else if clusterName != "" {
			return errors.Errorf("cannot perform initial write. Different cluster detected %s.", clusterName)
		}

		res, err := b.store.Bootstrap(cmd)
		if err != nil {
			log.WithError(err).Error(ctx, "Initial write failed. Retrying...")
			return err
		} else if res.Success {
			log.Info(ctx, "Initial write completed successfully.")
		} else {
			// this scenario is unlikeley, but if it does happen, it means that:
			//  - the current server lost the leadership,
			//  - then another server was elected group leader,
			//  - it performed successfully the initial write to the FSM,
			//  - then lost its leadership,
			//  - then the current node was elected leader back again,
			//  - and then performed the write above,
			//  - with all of this happening between the ClusterName and the Bootstrap calls.
			log.Warn(ctx, "Initial write was declined by the FSM.")
		}
		return nil
	})
}

func (b *Bootstrapper) storeIdentityWithRetry(ctx context.Context, p Params) error {
	return utils.RetryE(ctx, retryBackoff, func() error {
		if err := b.idStore.Set(p.identity); err != nil {
			log.WithError(err).Error(ctx, "Storing identity failed. Retrying...")
			return err
		} else {
			log.Info(ctx, "Stored identity successfully.")
			return nil
		}
	})
}

func (p *Params) Identity() identity.Identity {
	return p.identity
}
