package client

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/control"
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
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	attrKey = "lbconfig"
)

var (
	resolveThrottle = utils.AddJitter(2 * time.Second)
	resolveInterval = utils.AddJitter(5 * time.Second)
	logR            = logging.New("control_resolver")
)

type resolverBuilder struct {
	clusterName string
}

func (b *resolverBuilder) Build(t resolver.Target, clientConn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	discoveryTarget, err := discovery.GetDiscoveryTarget(t.URL)
	if err != nil {
		return nil, err
	}

	resolveNowCh, resolveNowChTh := utils.MakeThrottleChan[resolver.ResolveNowOptions](resolveThrottle, 1)

	r := &resolverImpl{
		clientConn:      clientConn,
		opts:            opts,
		clusterName:     b.clusterName,
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
	clientConn      resolver.ClientConn
	opts            resolver.BuildOptions
	clusterName     string
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
	ticker := time.NewTicker(resolveInterval)
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
	conn, client := r.createClient()
	if err := conn.Start(ctx); err != nil {
		logR.WithError(err).Warnf(ctx, "Failed to start control client for resolver")
		r.clientConn.ReportError(err)
		return false
	}
	defer conn.Stop()

	resp, err := client.Discover(ctx, &emptypb.Empty{})
	if err != nil {
		logR.WithError(err).Warnf(ctx, "Discover call failed")
		r.clientConn.ReportError(err)
		return false
	}

	if err := r.updateState(ctx, resp); err != nil {
		logR.WithError(err).Warn(ctx, "Failed to update resolver state")
		r.clientConn.ReportError(err)
		return false
	}

	return true
}

func (r *resolverImpl) createClient() (*rpc.Conn, control.ServiceClient) {
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(r.opts.DialCreds),
		grpc.WithCredentialsBundle(r.opts.CredsBundle),
		grpc.WithContextDialer(r.opts.Dialer),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(serviceconfig.DefaultServiceConfig().ToJson()),
	}

	conn := rpc.NewConn(r.discoveryTarget, r.clusterName, dialOpts...)
	client := control.NewServiceClient(conn)

	return conn, client
}

func (r *resolverImpl) updateState(ctx context.Context, resp *control.DiscoverResponse) error {
	leaderAddress, allAddresses, err := r.extractResp(ctx, resp)
	if err != nil {
		return err
	}

	// update discovery target with latest cluster servers
	r.discoveryTarget = routing.FormatTargetStatic(allAddresses)

	parseResult := r.clientConn.ParseServiceConfig(resp.ServiceConfigJson)
	if parseResult.Err != nil {
		return errors.Wrap(parseResult.Err, "ParseServiceConfig call failed")
	}

	attr := attributes.New(attrKey, leaderAddress)

	return r.clientConn.UpdateState(resolver.State{
		Addresses:     []resolver.Address{{Addr: "dummy_addr"}},
		ServiceConfig: parseResult,
		Attributes:    attr,
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
