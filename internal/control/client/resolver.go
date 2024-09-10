package client

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/routing"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"github.com/bcrusu/graph/internal/tracing"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

var (
	resolveThrottle = utils.AddJitter(2*time.Second, 0.15)
	resolveInterval = utils.AddJitter(5*time.Second, 0.15)
	logR            = logging.WithComponent("control_resolver")
)

type resolverBuilder struct{}

func (*resolverBuilder) Build(t resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	target, err := discovery.ParseTarget(t.URL)
	if err != nil {
		return nil, err
	}

	r := &resolverImpl{
		clientConn:   clientConn,
		opts:         opts,
		target:       target,
		resolveNowCh: make(chan resolver.ResolveNowOptions),
	}

	go r.mainLoop()
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (*resolverBuilder) Scheme() string {
	return discovery.Scheme
}

type resolverImpl struct {
	clientConn   resolver.ClientConn
	opts         resolver.BuildOptions
	target       discovery.Target
	resolveNowCh chan resolver.ResolveNowOptions
}

func (r *resolverImpl) ResolveNow(opt resolver.ResolveNowOptions) {
	r.resolveNowCh <- opt
}

func (r *resolverImpl) Close() {
	close(r.resolveNowCh)
}

func (r *resolverImpl) mainLoop() {
	var last time.Time
	resolving := false
	ticker := time.NewTicker(resolveInterval)
	defer ticker.Stop()

	reqCh := make(chan bool)
	resCh := make(chan bool)

	go func() {
		for range reqCh {
			r.resolveNow(tracing.NewContext())
			resCh <- true
		}
	}()

	resolve := func() {
		now := time.Now()
		if !resolving && last.Before(now.Add(-resolveThrottle)) {
			resolving = true
			reqCh <- true
			last = now
		}
	}

	for {
		select {
		case _, ok := <-r.resolveNowCh:
			if !ok {
				close(reqCh)
				<-resCh
				close(resCh)
				return
			}

			resolve()
		case <-resCh:
			resolving = false
		case <-ticker.C:
			resolve()
		}
	}
}

func (r *resolverImpl) resolveNow(ctx context.Context) {
	client := r.createClient()
	if err := client.Start(ctx); err != nil {
		logR.WithError(err).Warnf(ctx, "Failed to start control client for resolver")
		r.clientConn.ReportError(err)
		return
	}
	defer client.Stop(ctx)

	req := &control.DiscoverRequest{
		ClusterName: r.target.ClusterName,
	}

	resp, err := client.Discover(ctx, req)
	if err != nil {
		logR.WithError(err).Warnf(ctx, "Discover call failed")
		r.clientConn.ReportError(err)
		return
	}

	if err := r.updateState(ctx, resp); err != nil {
		logR.WithError(err).Warn(ctx, "Failed to update resolver state")
		r.clientConn.ReportError(err)
		return
	}
}

func (r *resolverImpl) createClient() ControlClient {
	cfg := serviceconfig.DefaultServiceConfig().WithLBRoundRobin()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(r.opts.DialCreds),
		grpc.WithCredentialsBundle(r.opts.CredsBundle),
		grpc.WithContextDialer(r.opts.Dialer),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(cfg.ToJson()),
	}

	opts := []Option{
		WithTarget(r.target),
		WithDialOptions(dialOpts...),
	}

	return NewClient(opts...)
}

func (r *resolverImpl) updateState(ctx context.Context, resp *control.DiscoverResponse) error {
	leaderAddress, allAddresses, err := r.extractResp(ctx, resp)
	if err != nil {
		return err
	}

	// update discovery target with latest cluster servers; maybe should not mutate the target struct?
	r.target.Discovery = routing.FormatTargetStatic(allAddresses)

	parseResult := r.clientConn.ParseServiceConfig(resp.ServiceConfigJson)
	if parseResult.Err != nil {
		return errors.Wrap(parseResult.Err, "ParseServiceConfig call failed")
	}

	return r.clientConn.UpdateState(resolver.State{
		Addresses:     []resolver.Address{{Addr: leaderAddress}},
		ServiceConfig: parseResult,
	})
}

func (r *resolverImpl) extractResp(ctx context.Context, resp *control.DiscoverResponse) (string, []string, error) {
	if resp == nil || len(resp.Servers) == 0 {
		return "", nil, errors.Error("Discover returned empty response")
	}

	var leader string
	leaderCount := 0
	addrs := make([]string, len(resp.Servers))

	for i, s := range resp.Servers {
		addrs[i] = s.Address
		if s.Leader {
			leader = s.Address
			leaderCount++
		}
	}

	if leaderCount == 0 {
		return "", nil, errors.Error("leader is missing from Discover response")
	} else if leaderCount > 1 {
		logR.Warnf(ctx, "Multiple leaders detected %d", leaderCount)
	}

	return leader, addrs, nil
}
