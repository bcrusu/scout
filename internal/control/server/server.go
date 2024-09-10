package server

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control/server/bootstrap"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.WithComponent("control_server")
)

type Server struct {
	config     Config
	bconfig    *BootstrapConfig
	components []utils.Lifecycle
}

func NewServer(config Config) *Server {
	return &Server{
		config: config,
	}
}

func NewServerForBootstrap(config Config, bconfig BootstrapConfig) *Server {
	return &Server{
		config:  config,
		bconfig: &bconfig,
	}
}

func (n *Server) Start(ctx context.Context) (err error) {
	if n.bconfig == nil {
		n.components, err = n.getComponents()
	} else {
		n.components, err = n.getComponentsForBootstrap()
	}

	if err != nil {
		return err
	}

	return utils.LifecycleStart(ctx, log, n.components...)
}

func (n *Server) getComponents() ([]utils.Lifecycle, error) {
	idStore, err := identity.NewStore(n.config.DataDir)
	if err != nil {
		return nil, err
	}

	id, ok := idStore.Get()
	if !ok {
		return nil, errors.Error("server identity not found; must bootstrap or join a cluster first.")
	}

	fsm := storage.NewFSM()
	dialOpts := rpc.DefaultDialOptions()
	transportService := multiraft.NewTransportService(n.config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	cfg := multiraft.Config{
		ID:             id.Name,
		Address:        n.config.Server.BindAddress,
		RequestTimeout: 2 * time.Second,
		Transport:      transportService.Transport("control"),
		FSM:            fsm,
	}

	raft := multiraft.NewRaft(cfg)

	controlService := NewControlService(raft, fsm)
	server := rpc.NewServer(n.config.Server, controlService, transportService)

	return []utils.Lifecycle{
		controlService,
		transportService,
		raft,
		server,
	}, nil
}

func (n *Server) getComponentsForBootstrap() ([]utils.Lifecycle, error) {
	idStore, err := identity.NewStore(n.config.DataDir)
	if err != nil {
		return nil, err
	}

	if id, ok := idStore.Get(); ok {
		return nil, errors.Errorf("selver is alredy part of cluster %s; cannot bootstrap.", id.ClusterName)
	}

	params := bootstrap.Params{
		ClusterName:  n.config.ClusterName,
		LocalAddress: n.bconfig.LocalAddress,
		Peers:        n.bconfig.Peers,
	}

	if err := bootstrap.ValidateParams(&params); err != nil {
		return nil, err
	}

	fsm := storage.NewFSM()
	dialOpts := rpc.DefaultDialOptions()
	transportService := multiraft.NewTransportService(n.config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	cfg := multiraft.Config{
		ID:             params.LocalName(),
		Address:        n.config.Server.BindAddress,
		RequestTimeout: 2 * time.Second,
		Transport:      transportService.Transport("control"),
		FSM:            fsm,
	}

	raft := multiraft.NewRaft(cfg)

	controlService := NewControlService(raft, fsm)
	server := rpc.NewServer(n.config.Server, controlService, transportService)
	bootstrapper := bootstrap.NewBootstrapper(raft, fsm, params)

	return []utils.Lifecycle{
		controlService,
		transportService,
		raft,
		server,
		bootstrapper,
	}, nil
}

func (n *Server) Stop(ctx context.Context) {
	utils.LifecycleStop(ctx, log, n.components...)
}
