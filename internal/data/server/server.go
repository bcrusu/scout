package server

import (
	"context"

	"github.com/bcrusu/graph/internal/data/server/storage"
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
	Server  rpc.ServerConfig
	DataDir string
}

type Server struct {
	config     Config
	raft       *multiraft.MultiRaft
	components []utils.Lifecycle
}

func NewServer(config Config) *Server {
	fsm := storage.NewFSM()
	dialOpts := rpc.DefaultDialOptions()
	transportService := multiraft.NewTransportService(config.Server.BindAddress, dialOpts...)

	// TODO: make configurable
	// cfg := multiraft.Config{
	// 	ID:             raft.ServerID(config.ServerID),
	// 	Address:        raft.ServerAddress(config.Server.BindAddress),
	// 	RequestTimeout: 2 * time.Second,
	// 	Transport:      transportService.Transport("TODO"),
	// 	FSM:            fsm,
	// }

	raft := multiraft.NewMultiRaft()

	dataService := NewDataService(nil, fsm)
	server := rpc.NewServer(config.Server, dataService, transportService)

	return &Server{
		config: config,
		raft:   raft,
		components: []utils.Lifecycle{
			dataService,
			transportService,
			raft,
			server,
		},
	}
}

func (n *Server) Start(ctx context.Context) error {
	return utils.LifecycleStart(ctx, log, n.components...)
}

func (n *Server) Stop(ctx context.Context) {
	utils.LifecycleStop(ctx, log, n.components...)
}
