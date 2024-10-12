package rpc

import (
	"context"
	"net"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/interceptors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_     utils.Lifecycle = (*Server)(nil)
	logRS                 = logging.WithComponent("rpc_server")
)

// ServerConfig is the gRPC server configuration.
type ServerConfig struct {
	ClusterName          string        `yaml:"clusterName" validate:"required,maxLen:100"`
	BindAddress          string        `yaml:"bindAddress" validate:"required,minLen:2,maxLen:128"`
	ShutdownTimeout      time.Duration `yaml:"shutdownTimeout" default:"5s" validate:"positive"`
	MaxConcurrentStreams uint32        `yaml:"maxConcurrentStreams" default:"10000" validate:"min:1000"`
	MaxMessageSize       utils.Bytes   `yaml:"maxMessageSize" default:"5MB" validate:"min:1KB"`
}

// Server represents the gRPC server.
type Server struct {
	config   ServerConfig
	services []Service
	server   *grpc.Server
}

// Service represents a service served by the gRPC server.
type Service interface {
	RegisterToServer(server *grpc.Server)
}

// NewServer returns a new Server instance.
func NewServer(config ServerConfig, services ...Service) *Server {
	options := []grpc.ServerOption{
		grpc.Creds(insecure.NewCredentials()), // TODO
		grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),
		grpc.MaxRecvMsgSize(int(config.MaxMessageSize.MustParse())),
		grpc.MaxSendMsgSize(int(config.MaxMessageSize.MustParse())),
		grpc.WaitForHandlers(true),
		grpc.ConnectionTimeout(10 * time.Second),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryAuthServerInterceptor(config.ClusterName),
			interceptors.UnaryMetadataServerInterceptor(),
			interceptors.UnaryLoggerServerInterceptor(),
			interceptors.UnaryErrorsServerInterceptor(),
			interceptors.UnaryHlcServerInterceptor(),
			interceptors.UnaryRecoveryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamAuthServerInterceptor(config.ClusterName),
			interceptors.StreamMetadataServerInterceptor(),
			interceptors.StreamLoggerServerInterceptor(),
			interceptors.StreamErrorsServerInterceptor(),
			interceptors.StreamHlcServerInterceptor(),
			interceptors.StreamRecoveryServerInterceptor(),
		),
	}

	server := grpc.NewServer(options...)

	for _, service := range services {
		service.RegisterToServer(server)
	}

	return &Server{
		config:   config,
		server:   server,
		services: services,
	}
}

// Start the server.
func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.BindAddress)
	if err != nil {
		return errors.Wrap(err, "failed to create TCP listener")
	}

	go func() {
		if err := s.server.Serve(listener); err != nil {
			logRS.WithError(err).Error(ctx, "Failed to serve")
		}
	}()

	return nil
}

// Stop the server, with no survivors.
func (s *Server) Stop() {
	stopped := make(chan any)
	go func() {
		s.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-time.After(s.config.ShutdownTimeout):
		// this is expected to happen since there will be active long-lived streams...
		logRS.NoContext().Debug("Failed to stop in the configured shutdown timeout.")
		s.server.Stop()
	case <-stopped:
	}
}
