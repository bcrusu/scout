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
	"github.com/hashicorp/raft"
)

const (
	raftGroupName = "control"
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

func (n *Server) Stop() {
	utils.LifecycleStop(log.NoContext(), n.components...)
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

	store, transportService, raft, err := n.buildRaft(id.ServerName)
	if err != nil {
		return nil, err
	}

	controlService := NewControlService(raft, store)
	server := rpc.NewServer(n.config.Server, controlService, transportService)

	return []utils.Lifecycle{
		store,
		transportService,
		raft,
		controlService,
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
		ClusterName:    n.config.ClusterName,
		LocalAddress:   n.bconfig.LocalAddress,
		Peers:          n.bconfig.Peers,
		PartitionCount: n.bconfig.PartitionCount,
	}

	if err := bootstrap.ValidateParams(&params); err != nil {
		return nil, err
	}

	store, transportService, raft, err := n.buildRaft(params.Identity().ServerName)
	if err != nil {
		return nil, err
	}

	controlService := NewControlService(raft, store)
	server := rpc.NewServer(n.config.Server, controlService, transportService)
	bootstrapper := bootstrap.NewBootstrapper(raft, store, idStore, params)

	return []utils.Lifecycle{
		store,
		transportService,
		raft,
		controlService,
		server,
		bootstrapper,
	}, nil
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
		FSM:            fsm,
	}

	mraft := multiraft.NewMultiRaft(config)

	raft, err := mraft.New(raftGroupName, raft.ServerID(serverName))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to create raft group")
	}

	store := storage.NewStore(raft, fsm)

	return store, transportService, raft, nil
}
