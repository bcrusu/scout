package server

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	cclient "github.com/bcrusu/scout/internal/control/client"
	dclient "github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/partitions"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/storage/inmem"
	"github.com/bcrusu/scout/internal/data/server/storage/rocksdb"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/http"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/metrics"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/register"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/session"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_   utils.Lifecycle = (*Server)(nil)
	log                 = logging.New("server")
)

type Server struct {
	config              config.Config
	components          []utils.Lifecycle
	partitionController *partitions.Controller
}

func NewServer() *Server {
	return &Server{
		config: config.Get(),
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

	id, ok := idStore.Get()
	if !ok {
		id, err = n.register(ctx, idStore, controlClient)
		if err != nil {
			return err
		}
	} else if id.ClusterName != n.config.ClusterName {
		return errors.Errorf("config cluster name differs from identity cluster name %s", id.ClusterName)
	}

	metrics := metrics.New(n.config.Metrics, id)
	session := session.New(id, n.config.Session, controlClient)
	dataClient := dclient.New(dclient.WithClusterName(id.ClusterName))
	multiraft := n.buildMultiRaft()
	db := n.buildDB()
	n.partitionController = partitions.NewController(id, db, multiraft, dataClient)
	dataService := NewDataService(n.partitionController)
	rpcServer := rpc.NewServer(n.config.RPC, dataService, multiraft)
	httpServer := http.NewServer(n.config.HTTP)

	n.components = []utils.Lifecycle{
		controlClient, // already started above, but will be needed during stop
		metrics,
		session,
		dataClient,
		multiraft,
		db,
		n.partitionController,
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
		ServerType:  control.ServerType_Data,
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

func (n *Server) buildMultiRaft() *multiraft.Multi {
	if n.config.InMem {
		return multiraft.NewInmem(n.config.Raft, n.config.ClusterName, n.config.RPC.Address)
	}

	return multiraft.New(n.config.Raft, n.config.RaftDir(), n.config.ClusterName, n.config.RPC.Address)
}

func (n *Server) buildDB() storage.DB {
	if n.config.InMem {
		return storage.NewDB(inmem.New())
	}

	return rocksdb.New()
}

func (n *Server) statusCallback() any {
	return &control.DataServerStatus{
		Replicas: n.partitionController.GetReplicaStatus(),
	}
}
