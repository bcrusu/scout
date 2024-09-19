package server

import (
	"context"
	"time"

	cclient "github.com/bcrusu/graph/internal/control/client"
	"github.com/bcrusu/graph/internal/data/server/partitions"
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
	Server      rpc.ServerConfig    `yaml:"server"`
	ClusterName string              `yaml:"clusterName"`
	DataDir     string              `yaml:"dataDir"`
	Discovery   discovery.Discovery `yaml:"discovery"`
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

	session := session.New(controlClient, id, n.config.Server.BindAddress)

	fsm, transportService, mraft, err := n.buildMultiRaft()
	if err != nil {
		return err
	}

	partitionController := partitions.NewController(id, mraft, fsm, session)
	dataService := NewDataService(partitionController)
	server := rpc.NewServer(n.config.Server, dataService, transportService)

	n.components = []utils.Lifecycle{
		controlClient,
		session,
		transportService,
		partitionController,
		server,
	}

	return utils.LifecycleStart(ctx, log, n.components...)
}

func (n *Server) Stop() {
	utils.LifecycleStop(log.NoContext(), n.components...)
}

func (n *Server) buildMultiRaft() (*storage.FSM, *multiraft.TransportService, *multiraft.MultiRaft, error) {
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

	return fsm, transportService, mraft, nil
}
