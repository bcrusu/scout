package multiraft

import (
	"context"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raft-grpc-transport/proto"
	"github.com/bcrusu/multiraft"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
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

func NewTransportService(config Config, localAddress string, dialOpts ...grpc.DialOption) *TransportService {
	opts := []transport.Option{
		transport.WithErrorLogger(func(err error, msg string, args ...any) {
			log.WithError(err).Error("Transport: "+msg, args...)
		}),
		transport.WithHeartbeatTimeout(config.HeartbeatTimeout),
	}

	scj := config.TransportClient.GetServiceConfigJson(serviceconfig.LBNameRoundRobin, proto.RaftTransport_ServiceDesc)

	dialOpts = append(dialOpts,
		grpc.WithDefaultServiceConfig(scj),
	)

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

func (t *TransportService) NewGroup(groupID string) (raft.Transport, error) {
	return t.manager.Transport(groupID), nil
}

func (t *TransportService) RemoveGroup(groupID string) error {
	return nil // TODO
}
