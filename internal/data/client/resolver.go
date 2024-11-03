package client

import (
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const (
	attrKey     = "lbconfig"
	dummyScheme = "scoutdata"
	dummyTarget = "scoutdata:scout"
)

var (
	logR = logging.New("data_resolver")
)

type resolverBuilder struct {
	options *options
}

func (b *resolverBuilder) Build(target resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	resolveNowCh, resolveNowChTh := utils.MakeThrottleChan[resolver.ResolveNowOptions](b.options.resolveThrottle, 1)

	r := &resolverImpl{
		options:        b.options,
		clientConn:     clientConn,
		buildOptions:   opts,
		resolveNowCh:   resolveNowCh,
		resolveNowChTh: resolveNowChTh,
	}

	go r.mainLoop()
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (b *resolverBuilder) Scheme() string {
	return dummyScheme
}

type resolverImpl struct {
	options        *options
	clientConn     resolver.ClientConn
	buildOptions   resolver.BuildOptions
	resolveNowCh   chan<- resolver.ResolveNowOptions
	resolveNowChTh <-chan resolver.ResolveNowOptions
}

func (r *resolverImpl) ResolveNow(opt resolver.ResolveNowOptions) {
	r.resolveNowCh <- opt
}

func (r *resolverImpl) Close() {
	close(r.resolveNowCh)
}

func (r *resolverImpl) mainLoop() {
	dataServersSub := eventbus.Subscribe[*control.DataServers]()
	defer dataServersSub.Unsubscribe()

	for {
		select {
		case _, ok := <-r.resolveNowChTh:
			if !ok {
				return
			}
			eventbus.TryPublishRefreshDataServers()
		case ds := <-dataServersSub.Items():
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

	cfg := balancerCfg{
		dataServers:       ds,
		reconnectInterval: r.options.reconnectInterval,
	}

	attr := attributes.New(attrKey, cfg)

	return r.clientConn.UpdateState(resolver.State{
		Addresses:     []resolver.Address{{Addr: "dummy_addr"}},
		ServiceConfig: parseResult,
		Attributes:    attr,
	})
}
