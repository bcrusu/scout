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
	Server      rpc.ServerConfig    `yaml:"server"`
	ClusterName string              `yaml:"clusterName"`
	DataDir     string              `yaml:"dataDir"`
	Discovery   discovery.Discovery `yaml:"discovery"`
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
		client.WithTarget(discovery.NewTarget(n.config.ClusterName, n.config.Discovery)),
	)

	n.components = []utils.Lifecycle{controlClient}

	switch n.action {
	case DoRegister:
		err = n.addComponentsForRegistration(idStore, controlClient)
	default:
		err = n.addComponents(idStore, controlClient)
	}

	if err != nil {
		return err
	}

	return utils.LifecycleStart(ctx, log, n.components...)
}

func (n *Server) addComponents(idStore identity.IdentityStore, client client.ControlClient) error {
	id, ok := idStore.Get()
	if !ok {
		return errors.Error("server identity not found; must join a cluster first.")
	}

	transportService, mraft, err := n.buildMultiRaft()
	if err != nil {
		return err
	}

	session := session.New(id, n.config.Server.BindAddress, client)
	partitionController := partitions.NewController(id, mraft)
	dataService := NewDataService(partitionController)
	server := rpc.NewServer(n.config.Server, dataService, transportService)

	n.components = append(n.components, session, transportService, partitionController, server)
	return nil
}

func (n *Server) addComponentsForRegistration(idStore identity.IdentityStore, client client.ControlClient) error {
	params := register.Params{
		ServerType:  control.ServerType_Data,
		ClusterName: n.config.ClusterName,
		BindAddress: n.config.Server.BindAddress,
	}

	n.components = append(n.components, register.NewRegisterer(idStore, client, params))
	return n.addComponents(idStore, client)
}

func (n *Server) Stop() {
	utils.LifecycleStop(log.NoContext(), n.components...)
}

func (n *Server) buildMultiRaft() (*multiraft.TransportService, *multiraft.MultiRaft, error) {
	dialOpts := rpc.DefaultDialOptions()
	transportService := multiraft.NewTransportService(n.config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	config := multiraft.Config{
		BindAddress:    n.config.Server.BindAddress,
		RequestTimeout: 2 * time.Second,
		Transport:      transportService,
	}

	mraft := multiraft.NewMultiRaft(config)

	return transportService, mraft, nil
}
