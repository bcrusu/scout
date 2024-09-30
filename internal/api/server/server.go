package server

import (
	"context"

	"github.com/bcrusu/graph/internal/api/server/graph"
	"github.com/bcrusu/graph/internal/api/server/keyvalue"
	"github.com/bcrusu/graph/internal/api/server/session"
	"github.com/bcrusu/graph/internal/api/server/txn"
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
	Server       rpc.ServerConfig    `yaml:"server"`
	DataDir      string              `yaml:"dataDir" validate:"required"`
	Discovery    discovery.Discovery `yaml:"discovery"`
	Transactions txn.Config          `yaml:"transactions"`
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
		cclient.WithClusterName(n.config.Server.ClusterName),
		cclient.WithDiscovery(n.config.Discovery),
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

	session := session.New(*id, n.config.Server.BindAddress, controlClient)
	dataClient := dclient.New(dclient.WithClusterName(id.ClusterName))
	txnProcessor := txn.NewProcessor(*id, n.config.Transactions, dataClient)
	adminService := NewAdminService(*id)
	keyValueService := NewKeyValueService(*keyvalue.NewStore(txnProcessor))
	graphService := NewGraphService(graph.NewStore(txnProcessor))
	server := rpc.NewServer(n.config.Server, adminService, keyValueService, graphService)

	n.components = []utils.Lifecycle{
		controlClient,
		session,
		dataClient,
		txnProcessor,
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
		ClusterName: n.config.Server.ClusterName,
		BindAddress: n.config.Server.BindAddress,
	}

	registerer := register.NewRegisterer(idStore, controlClient)
	return registerer.Register(ctx, params)
}
