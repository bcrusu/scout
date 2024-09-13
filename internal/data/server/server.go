package server

import (
	"context"

	cclient "github.com/bcrusu/graph/internal/control/client"
	dclient "github.com/bcrusu/graph/internal/data/client"
	"github.com/bcrusu/graph/internal/data/server/session"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.WithComponent("data_server")
)

type Config struct {
	Server      rpc.ServerConfig
	ClusterName string
	Discovery   discovery.DiscoveryTarget
	DataDir     string
}

type Server struct {
	config     Config
	components []utils.Lifecycle
}

func NewServer(config Config) *Server {
	return &Server{
		config: config,
	}
}

func (n *Server) Start(ctx context.Context) error {
	idStore, err := identity.NewStore(n.config.DataDir)
	if err != nil {
		return err
	}

	id, ok := idStore.Get()
	if !ok {
		return errors.Error("server identity not found; must join a cluster first.")
	}

	controlClient := cclient.New(
		cclient.WithTarget(discovery.NewTarget(n.config.ClusterName, n.config.Discovery)),
	)

	fsm := storage.NewFSM()
	dialOpts := rpc.DefaultDialOptions()
	transportService := multiraft.NewTransportService(n.config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	// cfg := multiraft.Config{
	// 	ID:             id.Name,
	// 	Address:        n.config.Server.BindAddress,
	// 	RequestTimeout: 2 * time.Second,
	// 	Transport:      transportService.Transport("control"),
	// 	FSM:            fsm,
	// }

	raft := multiraft.NewMultiRaft()

	session := session.New(controlClient, raft, id, n.config.Server.BindAddress)

	dataService := NewDataService(raft, fsm)
	server := rpc.NewServer(n.config.Server, dataService, transportService)

	dataClient := dclient.New(
		dclient.WithDataServers(session),
	)

	n.components = []utils.Lifecycle{
		controlClient,
		session,
		dataService,
		transportService,
		raft,
		dataClient,
		server,
	}

	return utils.LifecycleStart(ctx, log, n.components...)
}

func (n *Server) Stop(ctx context.Context) {
	utils.LifecycleStop(ctx, log, n.components...)
}
