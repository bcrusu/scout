package routing

import (
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

// ClientConn manages balancer.ClientConn sub connections.
// Should be invoked from gRPC Balancer methods to ensure sync access.
type ClientConn struct {
	balancer.ClientConn
	subConns          map[string]*subConn // map[address]subConn
	log               logging.LoggerNoContext
	stateChangedCb    func()
	reconnectInterval time.Duration
}

type subConn struct {
	parent      *ClientConn
	subConn     balancer.SubConn
	state       atomic.Int32 // connectivity.State
	shutdownCh  chan any
	connecting  atomic.Bool
	lastConnect time.Time
}

// NewClientConn returns a new ClientConn instance.
func NewClientConn(clientConn balancer.ClientConn) *ClientConn {
	return &ClientConn{
		ClientConn:        clientConn,
		subConns:          map[string]*subConn{},
		log:               logging.New("client_conns").NoContext(),
		stateChangedCb:    func() {},
		reconnectInterval: time.Second,
	}
}

func (c *ClientConn) Connect(addrs ...string) error {
	addrSet := utils.MakeSet(addrs)

	// close connections that are no longer necessary
	for addr, conn := range c.subConns {
		if !addrSet[addr] {
			delete(c.subConns, addr)
			conn.shutdown()
			c.log.Debug("Connection closed.", "address", addr)
		}
	}

	// open connections
	for _, addr := range addrs {
		if _, ok := c.subConns[addr]; ok {
			continue
		}

		log := c.log.With("address", addr)
		opts := balancer.NewSubConnOptions{
			StateListener: c.makeStateListener(addr, log),
		}

		newSubConn, err := c.NewSubConn([]resolver.Address{{Addr: addr}}, opts)
		if err != nil {
			log.WithError(err).Error("NewSubConn failed")
			return balancer.ErrBadResolverState
		}

		conn := &subConn{
			parent:     c,
			subConn:    newSubConn,
			shutdownCh: make(chan any),
		}
		conn.setState(connectivity.Idle)

		c.subConns[addr] = conn
		conn.connect()
		log.Debug("Connection created")
	}

	c.stateChangedCb()
	return nil
}

func (c *ClientConn) SubConn(addr string) balancer.SubConn {
	x := c.subConns[addr]
	if x != nil {
		return x.subConn
	}
	return nil
}

func (c *ClientConn) SubConnReady() []balancer.SubConn {
	ready := make([]balancer.SubConn, 0, len(c.subConns))

	for _, conn := range c.subConns {
		if conn.isReady() {
			ready = append(ready, conn.subConn)
		}
	}

	return ready
}

func (c *ClientConn) State(addr string) connectivity.State {
	conn := c.subConns[addr]
	if conn != nil {
		return conn.getState()
	}
	return connectivity.Idle
}

func (c *ClientConn) Count() int {
	return len(c.subConns)
}

func (c *ClientConn) IsReady(addr string) bool {
	return c.State(addr) == connectivity.Ready
}

func (c *ClientConn) SetLog(log logging.LoggerNoContext) {
	c.log = log
}

func (c *ClientConn) SetStateChangedCallback(cb func()) {
	c.stateChangedCb = cb
}

func (c *ClientConn) SetReconnectInterval(interval time.Duration) {
	c.reconnectInterval = interval
}

func (c *ClientConn) AggState() connectivity.State {
	if c.Count() == 0 {
		return connectivity.Idle
	}

	counts := map[connectivity.State]int{}

	for _, conn := range c.subConns {
		state := conn.getState()
		if state == connectivity.Ready {
			return connectivity.Ready
		}

		counts[state]++
	}

	switch {
	case counts[connectivity.Connecting] > 0:
		return connectivity.Connecting
	case counts[connectivity.TransientFailure] > 0:
		return connectivity.TransientFailure
	case counts[connectivity.Shutdown] == c.Count():
		return connectivity.Shutdown
	default:
		return connectivity.Idle
	}
}

func (c *ClientConn) makeStateListener(address string, log logging.LoggerNoContext) func(balancer.SubConnState) {
	return func(state balancer.SubConnState) {
		switch state.ConnectivityState {
		case connectivity.Idle:
			log.WithError(state.ConnectionError).Trace("Connection idle")
		case connectivity.Connecting:
			log.Trace("Connection connecting")
		case connectivity.Ready:
			log.Trace("Connection ready")
		case connectivity.Shutdown:
			log.Trace("Connection was shutdown")
		case connectivity.TransientFailure:
			log.WithError(state.ConnectionError).Warn("Transient failure")
		default:
			log.Warnf("Unexpected connectivity state %d", state.ConnectivityState)
		}

		// if subConn was removed, do not update state:
		conn, ok := c.subConns[address]
		if !ok {
			return
		}

		if state.ConnectionError != nil {
			log.Debug("Retrying connection...")
			conn.connect()
		}

		conn.setState(state.ConnectivityState)
		c.stateChangedCb()
	}
}

func (c *subConn) isReady() bool {
	return c.getState() == connectivity.Ready
}

func (c *subConn) getState() connectivity.State {
	return connectivity.State(c.state.Load())
}

func (c *subConn) setState(state connectivity.State) {
	c.state.Store(int32(state))
}

func (c *subConn) connect() {
	if c.connecting.Swap(true) {
		return
	}

	interval := utils.AddJitter(c.parent.reconnectInterval)
	diff := time.Since(c.lastConnect)

	if diff >= interval {
		c.connectNow()
		return
	}

	go func() {
		select {
		case <-c.shutdownCh:
			return
		case <-time.After(interval - diff):
			c.connectNow()
		}
	}()
}

func (c *subConn) connectNow() {
	c.subConn.Connect()
	c.lastConnect = time.Now()
	c.connecting.Store(false)
}

func (c *subConn) shutdown() {
	close(c.shutdownCh)
	c.subConn.Shutdown()
}
