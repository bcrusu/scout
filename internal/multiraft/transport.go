package multiraft

import (
	"context"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/bcrusu/multiraft"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

var (
	_ multiraft.Transport = (*TransportService)(nil)
	_ rpc.Service         = (*TransportService)(nil)
	_ utils.Lifecycle     = (*TransportService)(nil)
)

type TransportService struct {
	manager *transport.Manager
}

func NewTransportService(localAddress string, dialOpts ...grpc.DialOption) *TransportService {
	opts := []transport.Option{
		transport.WithErrorLogger(func(err error, msg string, args ...any) {
			log.WithError(err).Error("Transport: "+msg, args...)
		}),
		// TODO: transport.WithHeartbeatTimeout()
	}

	// TODO: add serviceconfig to dialOpts
	address := raft.ServerAddress(localAddress)
	manager := transport.New(address, dialOpts, opts...)

	return &TransportService{
		manager: manager,
	}
}

func (t *TransportService) Start(ctx context.Context) error {
	return nil
}

func (t *TransportService) Stop() {
	if err := t.manager.Close(); err != nil {
		log.WithError(err).Error("Transport manager failed to close")
	}
}

func (t *TransportService) RegisterToServer(server *grpc.Server) {
	t.manager.Register(server)
}

func (t *TransportService) CreateGroup(groupID string) (raft.Transport, error) {
	return t.manager.Transport(groupID), nil
}

func (t *TransportService) DeleteGroup(groupID string) error {
	return nil // TODO
}
