package client

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/api"
	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/rpc/routing"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/tracing"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const (
	attrKey = "lbconfig"
)

var (
	logR = logging.New("api_resolver")
)

type resolverBuilder struct {
	options *options
}

func (b *resolverBuilder) Build(t resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	discoveryTarget, err := discovery.GetDiscoveryTarget(t.URL)
	if err != nil {
		return nil, err
	}

	resolveNowCh, resolveNowChTh := utils.MakeThrottleChan[resolver.ResolveNowOptions](b.options.resolveThrottle, 1)

	r := &resolverImpl{
		options:         b.options,
		clientConn:      clientConn,
		buildOptions:    opts,
		discoveryTarget: discoveryTarget,
		resolveNowCh:    resolveNowCh,
		resolveNowChTh:  resolveNowChTh,
	}

	go r.mainLoop()
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (b *resolverBuilder) Scheme() string {
	return discovery.Scheme
}

type resolverImpl struct {
	options         *options
	clientConn      resolver.ClientConn
	buildOptions    resolver.BuildOptions
	discoveryTarget string
	resolveNowCh    chan<- resolver.ResolveNowOptions
	resolveNowChTh  <-chan resolver.ResolveNowOptions
}

func (r *resolverImpl) ResolveNow(opt resolver.ResolveNowOptions) {
	r.resolveNowCh <- opt
}

func (r *resolverImpl) Close() {
	close(r.resolveNowCh)
}

func (r *resolverImpl) mainLoop() {
	ticker := time.NewTicker(utils.AddJitter(r.options.resolveInterval))
	defer ticker.Stop()

	resolving := false
	reqCh := make(chan bool)
	resCh := make(chan bool)

	go func() {
		for range reqCh {
			resCh <- r.resolveNow(tracing.NewContext())
		}
	}()

	resolve := func() {
		if !resolving {
			resolving = true
			reqCh <- true
		}
	}

	for {
		select {
		case _, ok := <-r.resolveNowChTh:
			if !ok {
				close(reqCh)
				<-resCh
				close(resCh)
				return
			}

			resolve()
		case ok := <-resCh:
			resolving = false
			if ok {
				ticker.Stop()
			}
		case <-ticker.C:
			resolve()
		}
	}
}

func (r *resolverImpl) resolveNow(ctx context.Context) bool {
	log := logR.WithContext(ctx)

	conn, client := r.createClient()
	if err := conn.Start(ctx); err != nil {
		log.WithError(err).Warnf("Failed to start client for resolver")
		r.clientConn.ReportError(err)
		return false
	}
	defer conn.Stop()

	resp, err := client.Discover(ctx, &api.DiscoverRequest{})
	if err != nil {
		log.WithError(err).Warnf("Discover call failed")
		r.clientConn.ReportError(err)
		return false
	}

	if err := r.updateState(resp); err != nil {
		log.WithError(err).Warn("Failed to update resolver state")
		r.clientConn.ReportError(err)
		return false
	}

	return true
}

func (r *resolverImpl) createClient() (*rpc.Conn, api.ServiceClient) {
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(r.buildOptions.DialCreds),
		grpc.WithCredentialsBundle(r.buildOptions.CredsBundle),
		grpc.WithContextDialer(r.buildOptions.Dialer),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(serviceconfig.DefaultServiceConfig().ToJson()),
	}

	config := rpc.ConnConfig{
		Target:      r.discoveryTarget,
		ClusterName: r.options.clusterName,
		EnableHlc:   false,
	}

	conn := rpc.NewConn(config, dialOpts...)
	client := api.NewServiceClient(conn)

	return conn, client
}

func (r *resolverImpl) updateState(resp *api.DiscoverResponse) error {
	if !resp.ProxyMode && len(resp.Servers) > 0 {
		// update discovery target with latest servers
		r.discoveryTarget = routing.FormatTargetStatic(resp.Servers...)
	}

	parseResult := r.clientConn.ParseServiceConfig(resp.ServiceConfigJson)
	if parseResult.Err != nil {
		return errors.Wrap(parseResult.Err, "ParseServiceConfig call failed")
	}

	cfg := balancerCfg{
		proxyAddress:      r.options.address,
		response:          resp,
		reconnectInterval: r.options.reconnectInterval,
	}

	attr := attributes.New(attrKey, cfg)

	return r.clientConn.UpdateState(resolver.State{
		Addresses:     []resolver.Address{{Addr: "dummy_addr"}},
		ServiceConfig: parseResult,
		Attributes:    attr,
	})
}
