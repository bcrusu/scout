package server

import (
	"context"

	"github.com/bcrusu/scout/internal/api/server/config"
	"github.com/bcrusu/scout/internal/api/server/graph"
	"github.com/bcrusu/scout/internal/api/server/keyvalue"
	"github.com/bcrusu/scout/internal/api/server/txn"
	"github.com/bcrusu/scout/internal/control"
	cclient "github.com/bcrusu/scout/internal/control/client"
	dclient "github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/http"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/metrics"
	"github.com/bcrusu/scout/internal/register"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/session"
	"github.com/bcrusu/scout/internal/utils"
)

const (
	DoStart    Action = "start"
	DoRegister Action = "register"
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.New("server")
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
	idStore, err := n.buildIdentityStore()
	if err != nil {
		return err
	}

	controlClient := cclient.New(
		cclient.WithClusterName(n.config.ClusterName),
		cclient.WithDiscovery(n.config.Discovery),
	)

	if err := controlClient.Start(ctx); err != nil {
		return err
	}

	var id identity.Identity

	switch n.action {
	case DoRegister:
		id, err = n.register(ctx, idStore, controlClient)
		if err != nil {
			return err
		}
	default:
		var ok bool
		if id, ok = idStore.Get(); ok {
			return errors.Error("server identity not found; must join a cluster first.")
		} else if id.ClusterName != n.config.ClusterName {
			return errors.Errorf("cluster name differs from stored cluster name %s", id.ClusterName)
		}
	}

	metrics := metrics.New(n.config.Metrics, id)
	session := session.New(id, n.config.Session, controlClient)
	dataClient := dclient.New(dclient.WithClusterName(id.ClusterName))
	txnProcessor := txn.NewProcessor(id, dataClient)
	apiService := NewApiService(id)
	keyValueService := keyvalue.NewService(txnProcessor)
	graphService := graph.NewService(txnProcessor)
	rpcServer := rpc.NewServer(n.config.RPC, apiService, keyValueService, graphService)
	httpServer := http.NewServer(n.config.HTTP)

	n.components = []utils.Lifecycle{
		controlClient, // already started above, but will be needed during stop
		metrics,
		session,
		dataClient,
		txnProcessor,
		apiService,
		httpServer,
		rpcServer,
	}

	session.SetStatusCallback(n.statusCallback)

	return utils.LifecycleStart(ctx, log, n.components[1:]...)
}

func (n *Server) Stop() {
	utils.LifecycleStop(log, n.components...)
}

func (n *Server) register(ctx context.Context, idStore identity.Store, controlClient cclient.ControlClient) (identity.Identity, error) {
	params := register.Params{
		ServerType:  control.ServerType_Api,
		ClusterName: n.config.ClusterName,
		Address:     n.config.RPC.Address,
		Token:       n.config.Register.Token,
		Tags:        n.config.Register.Tags,
	}

	registerer := register.NewRegisterer(idStore, controlClient, n.config.Register.RetryBackoff)
	return registerer.Register(ctx, params)
}

func (n *Server) buildIdentityStore() (identity.Store, error) {
	if n.config.InMem {
		return identity.NewInmem(), nil
	}

	return identity.NewStore(n.config.IdentityFile())
}

func (n *Server) statusCallback() any {
	return &control.ApiServerStatus{}
}
