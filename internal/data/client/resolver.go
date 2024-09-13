package client

import (
	"github.com/bcrusu/graph/internal/logging"
	"google.golang.org/grpc/resolver"
)

const (
	dummyScheme = "graphdata"
	dummyTarget = "graphdata:graph"
)

var (
	logR = logging.WithComponent("data_resolver")
)

type resolverBuilder struct {
	dataServers DataServers
}

func (*resolverBuilder) Build(target resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	return nil, nil
}

func (*resolverBuilder) Scheme() string {
	return dummyScheme
}
