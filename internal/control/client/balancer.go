package client

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/routing"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"
)

var (
	logLB = logging.New("control_balancer").NoContext()
)

type balancerBuilder struct{}

type balancerImpl struct {
	conn          *routing.ClientConn
	leaderAddress string
	picker        *picker
}

type picker struct {
	conn          *routing.ClientConn
	leaderAddress string
}

type balancerCfg struct {
	leaderAddress     string
	reconnectInterval time.Duration
}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &balancerImpl{
		conn: routing.NewClientConn(clientConn),
	}
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameScoutControl
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
	} else if cfg.leaderAddress == b.leaderAddress {
		return nil
	}

	b.conn.SetLog(logLB)
	b.conn.SetReconnectInterval(cfg.reconnectInterval)
	b.conn.SetStateChangedCallback(b.updateState)

	b.leaderAddress = cfg.leaderAddress
	b.picker = &picker{b.conn, cfg.leaderAddress}

	return b.conn.Connect(cfg.leaderAddress)
}

func (b *balancerImpl) updateState() {
	b.conn.UpdateState(balancer.State{
		ConnectivityState: b.conn.AggState(),
		Picker:            b.picker,
	})
}

func (b *balancerImpl) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	logLB.Warn("Unexpected call to deprecated method UpdateSubConnState")
}

func (b *balancerImpl) ResolverError(err error) {}

func (b *balancerImpl) Close() {}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{
		SubConn: p.conn.SubConn(p.leaderAddress),
		Done:    p.rpcDone,
	}, nil
}

func (p *picker) rpcDone(d balancer.DoneInfo) {
	if errors.IsAny(d.Err, errors.Unavailable, errors.NotLeader) {
		// the resolver will throttle the ResolveNow calls
		p.conn.ResolveNow(resolver.ResolveNowOptions{})
	}
}
