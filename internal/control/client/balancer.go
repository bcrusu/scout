package client

import (
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

var (
	logB = logging.WithComponent("control_balancer").NoContext()
)

type balancerBuilder struct{}

type balancerImpl struct {
	clientConn  balancer.ClientConn
	subConnAddr string
	subConn     balancer.SubConn
	picker      *picker
}

type picker struct {
	balancer *balancerImpl
}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &balancerImpl{
		clientConn: clientConn,
	}
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameGraphControl
}

func (b *balancerImpl) UpdateClientConnState(state balancer.ClientConnState) error {
	if len(state.ResolverState.Addresses) == 0 {
		logB.Error("UpdateClientConnState invoked with empty addresses")
		return balancer.ErrBadResolverState
	} else if len(state.ResolverState.Addresses) > 1 {
		logB.Error("UpdateClientConnState invoked with mutiple addresses")
		return balancer.ErrBadResolverState
	}

	addr := state.ResolverState.Addresses[0].Addr
	log := logB.With("address", addr)

	if addr == b.subConnAddr {
		log.Debug("UpdateClientConnState invoked with the same address")
		return nil
	} else {
		b.closeSubConn()
	}

	opts := balancer.NewSubConnOptions{
		StateListener: func(state balancer.SubConnState) {
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

			if b.subConnAddr == addr {
				b.clientConn.UpdateState(balancer.State{
					ConnectivityState: state.ConnectivityState,
					Picker:            b.picker,
				})
			}
		},
	}

	subConn, err := b.clientConn.NewSubConn([]resolver.Address{{Addr: addr}}, opts)
	if err != nil {
		log.WithError(err).Error("NewSubConn failed")
		return balancer.ErrBadResolverState
	}

	log.Debug("Connection created")
	b.subConnAddr = addr
	b.subConn = subConn
	b.picker = &picker{balancer: b}

	subConn.Connect()
	log.Debug("Connection connected")
	return nil
}

func (b *balancerImpl) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	logB.Warn("Unexpected call to deprecated method UpdateSubConnState")
}

func (b *balancerImpl) ResolverError(err error) {}

func (b *balancerImpl) Close() {}

func (b *balancerImpl) closeSubConn() {
	if b.subConn == nil {
		return
	}

	subConn := b.subConn

	logB.Debug("Closing connection", "address", b.subConnAddr)
	b.subConnAddr = ""
	b.subConn = nil
	b.picker = nil

	subConn.Shutdown()
}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{
		SubConn: p.balancer.subConn,
		Done:    p.rpcDone,
	}, nil
}

func (p *picker) rpcDone(d balancer.DoneInfo) {
	if d.Err == errors.NotLeader {
		// the resolver will throttle the ResolveNow calls
		p.balancer.clientConn.ResolveNow(resolver.ResolveNowOptions{})
	}
}
