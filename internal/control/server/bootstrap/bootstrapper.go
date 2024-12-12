package bootstrap

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
)

const (
	serverNamePrefix = "boot_" // will use a dedicated prefix for initial control plane servers.
)

var (
	log = logging.New("bootstrap")
)

type Params struct {
	ClusterName    string
	LocalAddress   string
	InitialServers []Server
	PartitionCount uint32
	valid          bool
	identity       identity.Identity
}

type Server struct {
	Address string
	Tags    []string
	id      uint64
	name    string
}

// Bootstrapper is used only once, in the beginning of time, when a new baby cluster is born.
type Bootstrapper struct {
	store   storage.Store
	idStore identity.Store
	backoff utils.Backoff
}

// NewBootstrapper returns a new Bootstrapper.
func NewBootstrapper(store storage.Store, idStore identity.Store, backoff utils.Backoff) *Bootstrapper {
	return &Bootstrapper{
		store:   store,
		idStore: idStore,
		backoff: backoff,
	}
}

// ValidateParams ensures that we have everything required to start the show.
func ValidateParams(p *Params) error {
	if p == nil || !control.IsValidClusterName(p.ClusterName) || !control.IsValidAddress(p.LocalAddress) ||
		!control.IsValidPartitionCount(p.PartitionCount) {
		return errors.Error("invalid bootstrap parameters")
	}

	if containsDuplicates(p.InitialServers) {
		return errors.Error("initial server list contains duplicates.")
	}

	slices.SortFunc(p.InitialServers, func(a, b Server) int { return strings.Compare(a.Address, b.Address) })

	for i := range len(p.InitialServers) {
		id := uint64(i + 1)
		p.InitialServers[i].id = id
		p.InitialServers[i].name = fmt.Sprintf("%s%d", serverNamePrefix, id)
	}

	idx := slices.IndexFunc(p.InitialServers, func(s Server) bool { return s.Address == p.LocalAddress })
	if idx < 0 {
		return errors.Error("initial server list does not contain the local server.")
	}

	p.identity = identity.Identity{
		ClusterName: p.ClusterName,
		ServerID:    p.InitialServers[idx].id,
		ServerName:  p.InitialServers[idx].name,
		ServerType:  control.ServerType_Control,
	}

	p.valid = true
	return nil
}

// Bootstrap performs the initial Raft cluster configuration and then executes the very first
// write operation setting the initial state in the replicated log, thus ensuring that we have
// a good configuration. It will retry until quorum is reached or until context is canceled.
// In case of error, Bootstrap can be called again using exactly the same parameters from the
// original attempt.
func (b *Bootstrapper) Bootstrap(ctx context.Context, p Params) error {
	if !p.valid {
		return errors.Error("params were not validated")
	}

	if id, ok := b.idStore.Get(); ok {
		return errors.Errorf("cannot bootstrap, already part of cluster %s", id.ClusterName)
	}

	log := log.WithContext(ctx).With("cluster", p.ClusterName)
	log.Debug("Bootstrapping the raft cluster...")

	if err := b.bootstrapRaft(p); err != nil {
		log.WithError(err).Debug("Bootstrap raft cluster failed.")
		return err
	} else {
		log.WithError(err).Debug("Bootstrap raft cluster success.")
	}

	doneCh := make(chan any)

	cancelFunc := utils.RunAsync(ctx, func(ctx context.Context) {
		log.Debug("Performing initial write...")

		if err := b.initalWriteWithRetry(ctx, p); err != nil {
			log.WithError(err).Debug("Initial write failed.")
		} else {
			log.WithError(err).Debug("Initial write success.")
		}

		log.Debug("Storing identity...")

		if err := b.storeIdentityWithRetry(ctx, p); err != nil {
			log.WithError(err).Debug("Storing identity failed.")
		} else {
			log.WithError(err).Debug("Storing identity success.")
		}

		log.Debug("Bootstrap success.")
		close(doneCh)
	})

	select {
	case <-ctx.Done():
		cancelFunc()
		return ctx.Err()
	case <-doneCh:
		return nil
	}
}

func (b *Bootstrapper) bootstrapRaft(p Params) error {
	servers := make([]raft.Server, len(p.InitialServers))

	for i, server := range p.InitialServers {
		servers[i] = raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(server.name),
			Address:  raft.ServerAddress(server.Address),
		}
	}

	if err := b.store.Raft().Bootstrap(servers...); err != nil {
		return errors.Wrap(err, "bootstrap raft cluster failed")
	}

	return nil
}

func (b *Bootstrapper) initalWriteWithRetry(ctx context.Context, p Params) error {
	servers := make([]*storage.Bootstrap_Server, len(p.InitialServers))
	for i, server := range p.InitialServers {
		servers[i] = &storage.Bootstrap_Server{
			Id:      server.id,
			Name:    server.name,
			Address: server.Address,
			Tags:    server.Tags,
		}
	}

	cmd := &storage.Bootstrap{
		ClusterName:    p.ClusterName,
		Servers:        servers,
		PartitionCount: p.PartitionCount,
	}

	log := log.WithContext(ctx)

	return utils.RetryForeverE(ctx, &b.backoff, func() error {
		if clusterName := b.store.ClusterName(); clusterName == p.ClusterName {
			log.Info("Initial write was completed successfully by another server.")
			return nil
		} else if clusterName != "" {
			return errors.Errorf("cannot perform initial write. Different cluster detected %s.", clusterName)
		}

		if !b.store.Raft().IsLeader() {
			log.Debug("Not leader. Backing off...")
			return errors.NotLeader
		}

		res, err := b.store.Bootstrap(cmd)
		if err != nil {
			log.WithError(err).Error("Initial write failed. Retrying...")
			return err
		} else if res.Success {
			log.Info("Initial write completed successfully.")
		} else {
			// this scenario is unlikeley, but if it does happen, it means that:
			//  - the current server lost the leadership,
			//  - then another server was elected group leader,
			//  - it performed successfully the initial write to the FSM,
			//  - then lost its leadership,
			//  - then the current node was elected leader back again,
			//  - and then performed the write above,
			//  - with all of this happening between the ClusterName and the Bootstrap calls.
			log.Warn("Initial write was declined by the FSM.")
		}
		return nil
	})
}

func (b *Bootstrapper) storeIdentityWithRetry(ctx context.Context, p Params) error {
	log := log.WithContext(ctx)

	return utils.RetryForeverE(ctx, &b.backoff, func() error {
		if err := b.idStore.Set(p.identity); err != nil {
			log.WithError(err).Error("Storing identity failed. Retrying...")
			return err
		} else {
			log.Info("Stored identity successfully.")
			return nil
		}
	})
}

func (p *Params) Identity() identity.Identity {
	return p.identity
}

func containsDuplicates(servers []Server) bool {
	set := map[string]bool{}
	for _, server := range servers {
		if set[server.Address] {
			return true
		}
		set[server.Address] = true
	}

	return false
}
