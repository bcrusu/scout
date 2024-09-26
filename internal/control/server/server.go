package server

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/client"
	"github.com/bcrusu/graph/internal/control/server/bootstrap"
	"github.com/bcrusu/graph/internal/control/server/config"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/register"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
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

func NewServer(config config.Config, action Action) *Server {
	return &Server{
		config: config,
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
			ClusterName:    n.config.ClusterName,
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
	}

	if id == nil {
		return errors.Error("server identity not found; must bootstrap or join a cluster first.")
	}

	store, transportService, raft, err := n.buildRaft(id.ServerName)
	if err != nil {
		return err
	}

	controlService := NewControlService(n.config.Service, raft, store)
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
		client.WithTarget(discovery.NewTarget(n.config.ClusterName, n.config.Register.Discovery)),
	)
	if err := client.Start(ctx); err != nil {
		return nil, err
	}
	defer client.Stop()

	params := register.Params{
		ServerType:  control.ServerType_Control,
		ClusterName: n.config.ClusterName,
		BindAddress: n.config.Server.BindAddress,
	}

	registerer := register.NewRegisterer(idStore, client)
	return registerer.Register(ctx, params)
}

func (n *Server) buildRaft(serverName string) (storage.Store, *multiraft.TransportService, *multiraft.Raft, error) {
	fsm := storage.NewFSM()
	dialOpts := rpc.DefaultDialOptions()
	transportService := multiraft.NewTransportService(n.config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	config := multiraft.Config{
		BindAddress:    n.config.Server.BindAddress,
		RequestTimeout: 2 * time.Second,
		Transport:      transportService,
	}

	mraft := multiraft.NewMultiRaft(config)

	raft, err := mraft.New(raftGroupName, fsm, raft.ServerID(serverName))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to create raft group")
	}

	store := storage.NewStore(raft, fsm)

	return store, transportService, raft, nil
}
