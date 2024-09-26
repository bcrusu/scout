package server

import (
	"context"

	"github.com/bcrusu/graph/internal/api/server/session"
	"github.com/bcrusu/graph/internal/control"
	cclient "github.com/bcrusu/graph/internal/control/client"
	dclient "github.com/bcrusu/graph/internal/data/client"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
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
	log                 = logging.WithComponent("api_server")
)

type Action string

type Config struct {
	Server      rpc.ServerConfig    `yaml:"server"`
	ClusterName string              `yaml:"clusterName" validate:"required,maxLen:100"`
	DataDir     string              `yaml:"dataDir" validate:"required"`
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

	controlClient := cclient.New(
		cclient.WithTarget(discovery.NewTarget(n.config.ClusterName, n.config.Discovery)),
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
	}

	if id == nil {
		return errors.Error("server identity not found; must join a cluster first.")
	}

	session := session.New(*id, n.config.Server.BindAddress, controlClient)
	dataClient := dclient.New()
	adminService := NewAdminService(*id)
	keyValueService := NewKeyValueService()
	graphService := NewGraphService()
	server := rpc.NewServer(n.config.Server, adminService, keyValueService, graphService)

	n.components = []utils.Lifecycle{
		controlClient,
		session,
		dataClient,
		adminService,
		server,
	}

	return utils.LifecycleStart(ctx, log, n.components...)
}

func (n *Server) Stop() {
	utils.LifecycleStop(log.NoContext(), n.components...)
}

func (n *Server) register(ctx context.Context, idStore identity.IdentityStore, controlClient cclient.ControlClient) (*identity.Identity, error) {
	params := register.Params{
		ServerType:  control.ServerType_Api,
		ClusterName: n.config.ClusterName,
		BindAddress: n.config.Server.BindAddress,
	}

	registerer := register.NewRegisterer(idStore, controlClient)
	return registerer.Register(ctx, params)
}
