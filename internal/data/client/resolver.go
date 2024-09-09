package client

import (
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/logging"
	"google.golang.org/grpc/resolver"
)

var (
	logR = logging.WithComponent("data_resolver")
)

type resolverBuilder struct{}

func (*resolverBuilder) Build(target resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	return nil, nil
}

func (*resolverBuilder) Scheme() string {
	return discovery.Scheme
}
