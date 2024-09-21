package client

import (
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/events"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const (
	attrKey     = "lbconfig"
	dummyScheme = "graphdata"
	dummyTarget = "graphdata:graph"
)

var (
	resolveThrottle = utils.AddJitter(2*time.Second, 0.15)
	logR            = logging.WithComponent("data_resolver").NoContext()
)

type resolverBuilder struct{}

func (b *resolverBuilder) Build(target resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	resolveNowCh, resolveNowChTh := utils.MakeThrottleChan[resolver.ResolveNowOptions](resolveThrottle, 1)

	r := &resolverImpl{
		clientConn:     clientConn,
		opts:           opts,
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
	clientConn     resolver.ClientConn
	opts           resolver.BuildOptions
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
	dataServersSub := events.Subscribe[*control.DataServers]()
	defer dataServersSub.Unsubscribe()

	for {
		select {
		case _, ok := <-r.resolveNowChTh:
			if !ok {
				return
			}
			events.TryPublishRefreshDataServers()
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

	attr := attributes.New(attrKey, ds)

	return r.clientConn.UpdateState(resolver.State{
		Addresses:     []resolver.Address{{Addr: "dummy_addr"}},
		ServiceConfig: parseResult,
		Attributes:    attr,
	})
}
