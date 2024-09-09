package client

import (
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"google.golang.org/grpc/balancer"
)

var (
	logB = logging.WithComponent("data_balancer")
)

type balancerBuilder struct{}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return nil
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameGraphData
}
