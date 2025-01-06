package agent

import (
	"context"
	"net"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc/interceptors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_ utils.Lifecycle = (*Server)(nil)
)

type Server struct {
	service *service
	server  *grpc.Server
}

func NewServer() *Server {
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
		server: grpc.NewServer(options...),
	}
}

func (n *Server) Start(ctx context.Context) error {
	n.service = newService()
	if err := n.service.Start(ctx); err != nil {
		return err
	}

	RegisterServiceServer(n.server, n.service)

	bindAddress, err := utils.GetBindAddress()
	if err != nil {
		return err
	}

	addr := utils.JoinHostPort(bindAddress, Port)
	log.Debug("Listening...", "address", addr)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "failed to create listener for address %s", addr)
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
	n.service.Stop()
}
