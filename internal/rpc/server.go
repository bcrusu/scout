package rpc

import (
	"context"
	"net"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/interceptors"
	"github.com/bcrusu/scout/internal/rpc/stats"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultPort = 11001
)

var (
	_     utils.Lifecycle = (*Server)(nil)
	logRS                 = logging.New("rpc_server")
)

// ServerConfig is the gRPC server configuration.
type ServerConfig struct {
	Address              string        `yaml:"address" validate:"maxLen:128"`
	ConnectionTimeout    time.Duration `yaml:"connectionTimeout" default:"5s" validate:"positive"`
	ShutdownTimeout      time.Duration `yaml:"shutdownTimeout" default:"5s" validate:"positive"`
	MaxConcurrentStreams uint32        `yaml:"maxConcurrentStreams" default:"10000" validate:"min:1000"`
	MaxMessageSize       utils.Bytes   `yaml:"maxMessageSize" default:"5MB" validate:"min:1KB"`
	ClusterName          string        `yaml:"-"`
	EnableHlc            bool          `yaml:"-"` // API servers do not require HLC checks
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
		grpc.ConnectionTimeout(config.ConnectionTimeout),
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryAuthServerInterceptor(config.ClusterName),
			interceptors.UnaryMetadataServerInterceptor(),
			interceptors.UnaryLoggerServerInterceptor(),
			interceptors.UnaryErrorsServerInterceptor(),
			interceptors.UnaryHlcServerInterceptor(config.EnableHlc),
			interceptors.UnaryValidatorServerInterceptor(),
			interceptors.UnaryRecoveryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamAuthServerInterceptor(config.ClusterName),
			interceptors.StreamMetadataServerInterceptor(),
			interceptors.StreamLoggerServerInterceptor(),
			interceptors.StreamErrorsServerInterceptor(),
			interceptors.StreamHlcServerInterceptor(config.EnableHlc),
			interceptors.StreamValidatorServerInterceptor(),
			interceptors.StreamRecoveryServerInterceptor(),
		),
		grpc.StatsHandler(stats.NewServerHandler()),
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
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return errors.Wrap(err, "failed to create TCP listener")
	}

	go func() {
		if err := s.server.Serve(listener); err != nil {
			logRS.WithContext(ctx).WithError(err).Error("Failed to serve")
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
		logRS.Debug("Failed to stop in the configured shutdown timeout.")
		s.server.Stop()
	case <-stopped:
	}
}
