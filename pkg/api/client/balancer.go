package client

import (
	"sync/atomic"

	"github.com/bcrusu/scout/internal/api"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

var (
	logLB = logging.New("api_balancer").NoContext()
)

type balancerBuilder struct{}

type balancerImpl struct {
	clientConn balancer.ClientConn
	etag       string
	conns      map[string]balancer.SubConn
	connState  map[string]connectivity.State
}

type picker struct {
	balancer *balancerImpl
	conns    []balancer.SubConn
	rr       atomic.Int32
}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &balancerImpl{
		clientConn: clientConn,
		conns:      map[string]balancer.SubConn{},
		connState:  map[string]connectivity.State{},
	}
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameScoutApi
}

func (b *balancerImpl) UpdateClientConnState(state balancer.ClientConnState) error {
	cfg := state.ResolverState.Attributes.Value(attrKey)
	if cfg == nil {
		logLB.Error("UpdateClientConnState invoked with empty config.")
		return balancer.ErrBadResolverState
	}

	resp, ok := cfg.(*api.DiscoverResponse)
	if !ok {
		logLB.Error("UpdateClientConnState invoked with bad config.")
		return balancer.ErrBadResolverState
	}

	if b.etag == resp.ETag {
		return nil
	}

	addrs := utils.MakeSet(resp.Servers)

	// close connections that are no longer necessary
	for addr, conn := range b.conns {
		if !addrs[addr] {
			delete(b.conns, addr)
			delete(b.connState, addr)
			conn.Shutdown()
			logLB.Debug("Connection closed.", "address", addr)
		}
	}

	// open connections
	for addr := range addrs {
		if _, ok := b.conns[addr]; ok {
			continue
		}

		log := logLB.With("address", addr)
		opts := balancer.NewSubConnOptions{
			StateListener: b.makeStateListener(addr, log),
		}

		conn, err := b.clientConn.NewSubConn([]resolver.Address{{Addr: addr}}, opts)
		if err != nil {
			log.WithError(err).Error("NewSubConn failed")
			return balancer.ErrBadResolverState
		}

		b.conns[addr] = conn
		b.connState[addr] = connectivity.Idle
		conn.Connect()
		log.Debug("Connection created")
	}

	b.etag = resp.ETag
	b.updateState()
	return nil
}

func (b *balancerImpl) updateState() {
	b.clientConn.UpdateState(balancer.State{
		ConnectivityState: b.getLBConnectivityState(),
		Picker:            b.makePicker(),
	})
}

func (b *balancerImpl) makeStateListener(address string, log logging.LoggerNoContext) func(balancer.SubConnState) {
	return func(state balancer.SubConnState) {
		switch state.ConnectivityState {
		case connectivity.Idle:
			log.Debug("Connection idle")
		case connectivity.Connecting:
			log.Debug("Connection connecting")
		case connectivity.Ready:
			log.Debug("Connection ready")
		case connectivity.Shutdown:
			log.Debug("Connection was shutdown")
		case connectivity.TransientFailure:
			log.WithError(state.ConnectionError).Warn("Transient failure")
		default:
			log.Warnf("Unexpected connectivity state %d", state.ConnectivityState)
		}

		// if conn was removed
		if _, ok := b.conns[address]; !ok {
			return
		}

		b.connState[address] = state.ConnectivityState
		b.updateState()
	}
}

func (b *balancerImpl) makePicker() *picker {
	var ready []balancer.SubConn

	for addr, conn := range b.conns {
		if state := b.connState[addr]; state == connectivity.Ready {
			ready = append(ready, conn)
		}
	}

	return &picker{
		balancer: b,
		conns:    utils.ShuffleSlice(ready),
	}
}

func (b *balancerImpl) getLBConnectivityState() connectivity.State {
	if len(b.conns) == 0 {
		return connectivity.Idle
	}

	states := map[connectivity.State]int{}
	for _, state := range b.connState {
		states[state]++
	}

	if states[connectivity.Ready] > 0 {
		return connectivity.Ready
	}

	if states[connectivity.Connecting] >= states[connectivity.TransientFailure] {
		return connectivity.Connecting
	} else {
		return connectivity.TransientFailure
	}
}

func (b *balancerImpl) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	logLB.Warn("Unexpected call to deprecated method UpdateSubConnState")
}

func (b *balancerImpl) ResolverError(err error) {}

func (b *balancerImpl) Close() {}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.conns) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	idx := int(p.rr.Add(1)) % len(p.conns)

	return balancer.PickResult{
		SubConn: p.conns[idx],
		Done:    p.rpcDone,
	}, nil
}

func (p *picker) rpcDone(d balancer.DoneInfo) {
	if errors.Is(d.Err, errors.Unavailable) {
		// the resolver will throttle the ResolveNow calls
		p.balancer.clientConn.ResolveNow(resolver.ResolveNowOptions{})
	}
}
