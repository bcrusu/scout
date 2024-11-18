package client

import (
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/api"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/routing"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"
)

var (
	logLB = logging.New("api_balancer")
)

type balancerBuilder struct{}

type balancerImpl struct {
	conn *routing.ClientConn
	etag string
}

type picker struct {
	conn  *routing.ClientConn
	ready []balancer.SubConn
	rr    atomic.Int32
}

type balancerCfg struct {
	proxyAddress      string
	response          *api.DiscoverResponse
	reconnectInterval time.Duration
}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &balancerImpl{
		conn: routing.NewClientConn(clientConn),
	}
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameScoutApi
}

func (b *balancerImpl) UpdateClientConnState(state balancer.ClientConnState) error {
	cfgValue := state.ResolverState.Attributes.Value(attrKey)
	if cfgValue == nil {
		logLB.Error("UpdateClientConnState invoked with empty config.")
		return balancer.ErrBadResolverState
	}

	cfg, ok := cfgValue.(balancerCfg)
	if !ok {
		logLB.Error("UpdateClientConnState invoked with bad config.")
		return balancer.ErrBadResolverState
	} else if b.etag == cfg.response.ETag {
		return nil
	}

	b.conn.SetLog(logLB)
	b.conn.SetReconnectInterval(cfg.reconnectInterval)
	b.conn.SetStateChangedCallback(b.updateState)

	b.etag = cfg.response.ETag

	if cfg.response.ProxyMode {
		logLB.Debug("Running in proxy mode.", "proxy", cfg.proxyAddress)
		return b.conn.Connect(cfg.proxyAddress)
	}

	logLB.Debug("Running in load-balanced mode.", "servers", cfg.response.Servers)
	return b.conn.Connect(cfg.response.Servers...)
}

func (b *balancerImpl) updateState() {
	picker := &picker{
		conn:  b.conn,
		ready: utils.ShuffleSlice(b.conn.SubConnReady()),
	}

	b.conn.UpdateState(balancer.State{
		ConnectivityState: b.conn.AggState(),
		Picker:            picker,
	})
}

func (b *balancerImpl) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	logLB.Warn("Unexpected call to deprecated method UpdateSubConnState")
}

func (b *balancerImpl) ResolverError(err error) {}

func (b *balancerImpl) Close() {}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.ready) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	idx := int(p.rr.Add(1)) % len(p.ready)

	return balancer.PickResult{
		SubConn: p.ready[idx],
		Done:    p.rpcDone,
	}, nil
}

func (p *picker) rpcDone(d balancer.DoneInfo) {
	if errors.Is(d.Err, errors.Unavailable) {
		// the resolver will throttle the ResolveNow calls
		p.conn.ResolveNow(resolver.ResolveNowOptions{})
	}
}
