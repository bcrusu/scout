package client

import (
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const (
	attrKey     = "lbconfig"
	dummyScheme = "graphdata"
	dummyTarget = "graphdata:graph"
)

var (
	logR = logging.WithComponent("data_resolver").NoContext()
)

type resolverBuilder struct {
	dataServers DataServers
}

func (b *resolverBuilder) Build(target resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &resolverImpl{
		clientConn:   clientConn,
		opts:         opts,
		dataServers:  b.dataServers,
		resolveNowCh: make(chan resolver.ResolveNowOptions),
	}

	go r.mainLoop()
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (b *resolverBuilder) Scheme() string {
	return dummyScheme
}

type resolverImpl struct {
	clientConn   resolver.ClientConn
	opts         resolver.BuildOptions
	resolveNowCh chan resolver.ResolveNowOptions
	dataServers  DataServers
}

func (r *resolverImpl) ResolveNow(opt resolver.ResolveNowOptions) {
	r.resolveNowCh <- opt
}

func (r *resolverImpl) Close() {
	close(r.resolveNowCh)
}

func (r *resolverImpl) mainLoop() {
	subscription := r.dataServers.SubscribeDataServers()

	for {
		select {
		case _, ok := <-r.resolveNowCh:
			if !ok {
				subscription.Unsubscribe()
				return
			}
			subscription.NotifyPublisher()
		case ds := <-subscription.ItemChan():
			if err := r.updateState(ds); err != nil {
				logR.WithError(err).Warn("Failed to update resolver state.")
				r.clientConn.ReportError(err)
			}
		}
	}
}

func (r *resolverImpl) updateState(ds *control.DataServers) error {
	parseResult := r.clientConn.ParseServiceConfig(ds.ServiceConfigJson)
	if parseResult.Err != nil {
		return errors.Wrap(parseResult.Err, "ParseServiceConfig call failed")
	}

	attr := attributes.New(attrKey, ds)

	return r.clientConn.UpdateState(resolver.State{
		Addresses:     []resolver.Address{{Addr: "dummy_addr"}},
		ServiceConfig: parseResult,
		Attributes:    attr,
	})
}
