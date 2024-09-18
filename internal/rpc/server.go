package rpc

import (
	"context"
	"net"
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/interceptors"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_     utils.Lifecycle = (*Server)(nil)
	logRS                 = logging.WithComponent("rpc_server")
)

// ServerConfig is the server configuration
type ServerConfig struct {
	BindAddress          string
	ShutdownTimeout      time.Duration
	MaxConcurrentStreams uint32
	MaxRecvMsgSize       uint64
	MaxSendMsgSize       uint64
}

// Server represents the gRPC server
type Server struct {
	config   ServerConfig
	services []Service
	server   *grpc.Server
}

// Service represents a service served by the gRPC server
type Service interface {
	RegisterToServer(server *grpc.Server)
}

// NewServer returns a new Server instance
func NewServer(config ServerConfig, services ...Service) *Server {
	options := []grpc.ServerOption{
		grpc.Creds(insecure.NewCredentials()), // TODO
		grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),
		grpc.MaxRecvMsgSize(int(config.MaxRecvMsgSize)),
		grpc.MaxSendMsgSize(int(config.MaxSendMsgSize)),
		grpc.WaitForHandlers(true),
		grpc.ConnectionTimeout(10 * time.Second),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryMetadataServerInterceptor(),
			interceptors.UnaryLoggerServerInterceptor(),
			interceptors.UnaryErrorsServerInterceptor(),
			interceptors.UnaryRecoveryServerInterceptor()),
		grpc.ChainStreamInterceptor(
			interceptors.StreamMetadataServerInterceptor(),
			interceptors.StreamLoggerServerInterceptor(),
			interceptors.StreamErrorsServerInterceptor(),
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

// Start the server
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

// Stop the server
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
