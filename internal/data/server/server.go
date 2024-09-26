package server

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/client"
	"github.com/bcrusu/graph/internal/data/server/partitions"
	"github.com/bcrusu/graph/internal/data/server/session"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/register"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
)

const (
	DoStart    Action = "start"
	DoRegister Action = "register"
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.WithComponent("data_server")
)

type Action string

type Config struct {
	Server    rpc.ServerConfig    `yaml:"server"`
	DataDir   string              `yaml:"dataDir" validate:"required"`
	Discovery discovery.Discovery `yaml:"discovery"`
}

type Server struct {
	config     Config
	action     Action
	components []utils.Lifecycle
}

func NewServer(config Config, action Action) *Server {
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

	controlClient := client.New(
		client.WithClusterName(n.config.Server.ClusterName),
		client.WithDiscovery(n.config.Discovery),
	)

	var id *identity.Identity

	switch n.action {
	case DoRegister:
		if err := controlClient.Start(ctx); err != nil {
			return err
		}

		id, err = n.register(ctx, idStore, controlClient)
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

	transportService, mraft := n.buildMultiRaft(*id)
	session := session.New(*id, n.config.Server.BindAddress, controlClient)
	partitionController := partitions.NewController(*id, mraft)
	dataService := NewDataService(partitionController)
	server := rpc.NewServer(n.config.Server, dataService, transportService)

	n.components = []utils.Lifecycle{
		controlClient,
		transportService,
		session,
		partitionController,
		server,
	}

	return utils.LifecycleStart(ctx, log, n.components...)
}

func (n *Server) Stop() {
	utils.LifecycleStop(log.NoContext(), n.components...)
}

func (n *Server) register(ctx context.Context, idStore identity.IdentityStore, controlClient client.ControlClient) (*identity.Identity, error) {
	params := register.Params{
		ServerType:  control.ServerType_Data,
		ClusterName: n.config.Server.ClusterName,
		BindAddress: n.config.Server.BindAddress,
	}

	registerer := register.NewRegisterer(idStore, controlClient)
	return registerer.Register(ctx, params)
}

func (n *Server) buildMultiRaft(id identity.Identity) (*multiraft.TransportService, *multiraft.MultiRaft) {
	dialOpts := rpc.DefaultDialOptions(id.ClusterName)
	transportService := multiraft.NewTransportService(n.config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	config := multiraft.Config{
		BindAddress:    n.config.Server.BindAddress,
		RequestTimeout: 2 * time.Second,
		Transport:      transportService,
	}

	mraft := multiraft.NewMultiRaft(config)

	return transportService, mraft
}
