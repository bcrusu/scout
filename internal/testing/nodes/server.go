package nodes

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc/interceptors"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/validation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_ utils.Lifecycle = (*Server)(nil)
)

type Config struct {
	SocketPath      string
	FirecrackerPath string `validate:"exists:file"`
	NodesDir        string `validate:"exists:dir"`
	KernelImage     string `validate:"exists:file"`
	KernelArgs      string `validate:"required"`
	RootFS          string `validate:"exists:file"`
	ScoutFS         string `validate:"exists:file"`
	WorkFS          string `validate:"exists:file"`
	NodeCPU         int    `validate:"min:1,max:8"`
	NodeMemory      int    `validate:"min:256,max:8192"`
	NetNSDir        string `validate:"exists:dir"`
	CNIBinDir       string `validate:"exists:dir"`
	CNIConfDir      string `validate:"exists:dir"`
	CNICacheDir     string `validate:"exists:dir"`
	CNINetworkName  string `validate:"required"`
	LogLevel        string
}

type Server struct {
	config Config
	server *grpc.Server
}

func NewServer(config Config) *Server {
	options := []grpc.ServerOption{
		grpc.Creds(insecure.NewCredentials()),
		grpc.ConnectionTimeout(time.Second),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryLoggerServerInterceptor(),
			interceptors.UnaryErrorsServerInterceptor(),
			interceptors.UnaryValidatorServerInterceptor(),
			interceptors.UnaryRecoveryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamLoggerServerInterceptor(),
			interceptors.StreamErrorsServerInterceptor(),
			interceptors.StreamValidatorServerInterceptor(),
			interceptors.StreamRecoveryServerInterceptor(),
		),
	}

	return &Server{
		config: config,
		server: grpc.NewServer(options...),
	}
}

func (n *Server) Start(ctx context.Context) error {
	if err := validation.Validate(n.config); err != nil {
		return err
	}

	service, err := newService(n.config)
	if err != nil {
		return errors.Wrap(err, "failed to create service")
	}

	RegisterServiceServer(n.server, service)

	if err := os.Remove(n.config.SocketPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to remove old socket file")
	}

	log.Debug("Listening...", "socket", n.config.SocketPath)

	listener, err := net.Listen("unix", n.config.SocketPath)
	if err != nil {
		return errors.Wrapf(err, "failed to create listener for socket %s", n.config.SocketPath)
	}

	if err := os.Chmod(n.config.SocketPath, 0777); err != nil {
		return errors.Wrap(err, "failed to chmod socket file")
	}

	go func() {
		if err := n.server.Serve(listener); err != nil {
			log.WithContext(ctx).WithError(err).Error("Failed to serve")
		}
	}()

	return nil
}

func (n *Server) Stop() {
	n.server.Stop()
}
