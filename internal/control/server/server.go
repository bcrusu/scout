package server

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/client"
	"github.com/bcrusu/scout/internal/control/server/bootstrap"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/http"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/metrics"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/register"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
)

const (
	raftId uint32 = 888
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.New("server")
)

type Server struct {
	config     config.Config
	components []utils.Lifecycle
}

func NewServer() *Server {
	return &Server{
		config: config.Get(),
	}
}

func (n *Server) Start(ctx context.Context) error {
	idStore, err := n.buildIdentityStore()
	if err != nil {
		return err
	}

	var bparams *bootstrap.Params

	id, ok := idStore.Get()
	if !ok {
		switch {
		case n.config.Bootstrap != nil:
			bparams = n.buildBootstrapParams()

			if err := bootstrap.ValidateParams(bparams); err != nil {
				return err
			}

			id = bparams.Identity()
		case n.config.Register != nil:
			id, err = n.register(ctx, idStore)
			if err != nil {
				return err
			}
		default:
			return errors.Error("server identity not found; must join a cluster first.")
		}
	} else if id.ClusterName != n.config.ClusterName {
		return errors.Errorf("config cluster name differs from identity cluster name %s", id.ClusterName)
	}

	metrics := metrics.New(n.config.Metrics, id)

	multiraft := n.buildMultiRaft()
	if err := multiraft.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start multiraft")
	}

	store, err := n.buildStore(id, multiraft)
	if err != nil {
		return err
	}

	controlService := NewControlService(store)
	rpcServer := rpc.NewServer(n.config.RPC, controlService, multiraft)
	httpServer := http.NewServer(n.config.HTTP)

	n.components = []utils.Lifecycle{
		multiraft, // already started above, but will be needed during stop
		metrics,
		store,
		controlService,
		httpServer,
		rpcServer,
	}

	if err := utils.LifecycleStart(ctx, log, n.components[1:]...); err != nil {
		return err
	}

	if bparams != nil {
		bootstrapper := bootstrap.NewBootstrapper(store, idStore, n.config.Bootstrap.RetryBackoff)
		return bootstrapper.Bootstrap(ctx, *bparams)
	}

	return nil
}

func (n *Server) Stop() {
	utils.LifecycleStop(log, n.components...)
}

func (n *Server) register(ctx context.Context, idStore identity.Store) (identity.Identity, error) {
	client := client.New(
		client.WithClusterName(n.config.ClusterName),
		client.WithDiscovery(n.config.Register.Discovery),
	)

	if err := client.Start(ctx); err != nil {
		return identity.Identity{}, err
	}
	defer client.Stop()

	params := register.Params{
		ServerType:  control.ServerType_Control,
		ClusterName: n.config.ClusterName,
		Address:     n.config.RPC.Address,
		Token:       n.config.Register.Token,
		Tags:        n.config.Register.Tags,
	}

	registerer := register.NewRegisterer(idStore, client, n.config.Register.RetryBackoff)
	return registerer.Register(ctx, params)
}

func (n *Server) buildIdentityStore() (identity.Store, error) {
	if n.config.InMem {
		return identity.NewInmem(), nil
	}

	return identity.NewStore(n.config.IdentityFile())
}

func (n *Server) buildMultiRaft() *multiraft.Multi {
	if n.config.InMem {
		return multiraft.NewInmem(n.config.Raft, n.config.ClusterName, n.config.RPC.Address)
	}

	return multiraft.New(n.config.Raft, n.config.RaftDir(), n.config.ClusterName, n.config.RPC.Address)
}

func (n *Server) buildStore(id identity.Identity, multi *multiraft.Multi) (storage.Store, error) {
	fsm := storage.NewFSM()

	raft, err := multi.New(raftId, fsm, raft.ServerID(id.ServerName))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft instance")
	}

	store := storage.NewStore(raft, fsm)
	return store, nil
}

func (n *Server) buildBootstrapParams() *bootstrap.Params {
	servers := make([]bootstrap.Server, len(n.config.Bootstrap.InitialServers))
	for i, server := range n.config.Bootstrap.InitialServers {
		servers[i] = bootstrap.Server{
			Address: server.Address,
			Tags:    server.Tags,
		}
	}

	return &bootstrap.Params{
		ClusterName:    n.config.ClusterName,
		LocalAddress:   n.config.RPC.Address,
		InitialServers: servers,
		PartitionCount: n.config.Bootstrap.PartitionCount,
	}
}
