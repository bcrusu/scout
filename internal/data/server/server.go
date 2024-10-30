package server

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	cclient "github.com/bcrusu/scout/internal/control/client"
	dclient "github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/partitions"
	"github.com/bcrusu/scout/internal/data/server/session"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/storage/inmem"
	"github.com/bcrusu/scout/internal/data/server/storage/rocksdb"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/register"
	"github.com/bcrusu/scout/internal/rpc"
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

	session := session.New(id, n.config.Server.BindAddress, controlClient)
	dataClient := dclient.New(dclient.WithClusterName(id.ClusterName))
	multiraft := n.buildMultiRaft()
	db := n.buildDB()
	partitionController := partitions.NewController(id, db, multiraft, dataClient)
	dataService := NewDataService(partitionController)
	server := rpc.NewServer(n.config.Server, n.config.ClusterName, dataService, multiraft)

	n.components = []utils.Lifecycle{
		controlClient,
		session,
		dataClient,
		multiraft,
		db,
		partitionController,
		server,
	}

	return utils.LifecycleStart(ctx, log, n.components[1:]...)
}

func (n *Server) Stop() {
	utils.LifecycleStop(log.NoContext(), n.components...)
}

func (n *Server) register(ctx context.Context, idStore identity.Store, controlClient cclient.ControlClient) (identity.Identity, error) {
	params := register.Params{
		ServerType:  control.ServerType_Data,
		ClusterName: n.config.ClusterName,
		BindAddress: n.config.Server.BindAddress,
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
		return multiraft.NewInmem(n.config.Raft, n.config.ClusterName, n.config.Server.BindAddress)
	}

	return multiraft.New(n.config.Raft, n.config.RaftDir(), n.config.ClusterName, n.config.Server.BindAddress)
}

func (n *Server) buildDB() storage.DB {
	if n.config.InMem {
		return storage.NewDB(inmem.New())
	}

	return rocksdb.New()
}
