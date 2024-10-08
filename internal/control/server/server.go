package server

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/client"
	"github.com/bcrusu/scout/internal/control/server/bootstrap"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/register"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
)

const (
	raftGroupName        = "control"
	DoStart       Action = "start"
	DoBootstrap   Action = "bootstrap"
	DoRegister    Action = "register"
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.WithComponent("control_server")
)

type Action string

type Server struct {
	config     config.Config
	action     Action
	components []utils.Lifecycle
}

func NewServer(action Action) *Server {
	return &Server{
		config: config.Get(),
		action: action,
	}
}

func (n *Server) Start(ctx context.Context) error {
	idStore, err := identity.NewStore(n.config.DataDir)
	if err != nil {
		return err
	}

	var bparams *bootstrap.Params
	var id *identity.Identity

	switch n.action {
	case DoBootstrap:
		bparams = &bootstrap.Params{
			ClusterName:    n.config.Server.ClusterName,
			LocalAddress:   n.config.Server.BindAddress,
			InitialServers: n.config.Bootstrap.InitialServers,
			PartitionCount: n.config.Bootstrap.PartitionCount,
		}

		if err := bootstrap.ValidateParams(bparams); err != nil {
			return err
		}

		id = utils.PointerOf(bparams.Identity())
	case DoRegister:
		id, err = n.register(ctx, idStore)
		if err != nil {
			return err
		}
	default:
		id = idStore.Get()
		if id == nil {
			return errors.Error("server identity not found; must join a cluster first.")
		} else if id.ClusterName != n.config.Server.ClusterName {
			return errors.Errorf("cluster name differs from stored cluster name %s", id.ClusterName)
		}
	}

	store, transportService, raft, err := n.buildRaft(*id)
	if err != nil {
		return err
	}

	controlService := NewControlService(raft, store)
	server := rpc.NewServer(n.config.Server, controlService, transportService)

	n.components = []utils.Lifecycle{
		store,
		transportService,
		raft,
		controlService,
		server,
	}

	if err := utils.LifecycleStart(ctx, log, n.components...); err != nil {
		return err
	}

	if bparams != nil {
		bootstrapper := bootstrap.NewBootstrapper(raft, store, idStore)
		return bootstrapper.Bootstrap(ctx, *bparams)
	}

	return nil
}

func (n *Server) Stop() {
	utils.LifecycleStop(log.NoContext(), n.components...)
}

func (n *Server) register(ctx context.Context, idStore identity.IdentityStore) (*identity.Identity, error) {
	client := client.New(
		client.WithClusterName(n.config.Server.ClusterName),
		client.WithDiscovery(n.config.Register.Discovery),
	)

	if err := client.Start(ctx); err != nil {
		return nil, err
	}
	defer client.Stop()

	params := register.Params{
		ServerType:  control.ServerType_Control,
		ClusterName: n.config.Server.ClusterName,
		BindAddress: n.config.Server.BindAddress,
	}

	registerer := register.NewRegisterer(idStore, client)
	return registerer.Register(ctx, params)
}

func (n *Server) buildRaft(id identity.Identity) (storage.Store, *multiraft.TransportService, *multiraft.Raft, error) {
	fsm := storage.NewFSM()
	dialOpts := rpc.DefaultDialOptions(id.ClusterName)
	transportService := multiraft.NewTransportService(n.config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	config := multiraft.Config{
		BindAddress:    n.config.Server.BindAddress,
		RequestTimeout: 2 * time.Second,
		Transport:      transportService,
	}

	mraft := multiraft.NewMultiRaft(config)

	raft, err := mraft.New(raftGroupName, fsm, raft.ServerID(id.ServerName))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to create raft group")
	}

	store := storage.NewStore(raft, fsm)

	return store, transportService, raft, nil
}
