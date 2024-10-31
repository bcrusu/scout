package multiraft

import (
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raft-grpc-transport/proto"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

func newTransport(config Config, clusterName, localAddress string) *transport.Manager {
	opts := []transport.Option{
		transport.WithErrorLogger(func(err error, msg string, args ...any) {
			log.WithError(err).Error("Transport: "+msg, args...)
		}),
		transport.WithHeartbeatTimeout(config.HeartbeatTimeout),
	}

	scj := config.Transport.GetServiceConfigJson(serviceconfig.LBNameRoundRobin, proto.RaftTransport_ServiceDesc)

	dialOpts := append(rpc.DefaultDialOptions(clusterName, true),
		grpc.WithDefaultServiceConfig(scj),
	)

	address := raft.ServerAddress(localAddress)
	manager := transport.New(address, dialOpts, opts...)

	return manager
}
