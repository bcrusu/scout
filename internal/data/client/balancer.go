package client

import (
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

var (
	logB = logging.WithComponent("data_balancer").NoContext()
)

type balancerBuilder struct{}

type balancerImpl struct {
	clientConn    balancer.ClientConn
	subConns      map[string]balancer.SubConn   // map[address]SubCon
	subConnsState map[string]connectivity.State // map[address]State
	serverAddress map[uint64]string             // map[server_id]address
	partitions    map[uint32]partition          // map[part_id]part
	picker        *picker
}

type picker struct {
	balancer   *balancerImpl
	partitions map[uint32]partition // map[part_id]part
}

type partition struct {
	readServerIDs []uint64
	readConns     []balancer.SubConn
	writeServerID uint64
	writeConn     balancer.SubConn
}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &balancerImpl{
		clientConn:    clientConn,
		subConns:      map[string]balancer.SubConn{},
		subConnsState: map[string]connectivity.State{},
		serverAddress: map[uint64]string{},
	}
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameGraphData
}

func (b *balancerImpl) UpdateClientConnState(state balancer.ClientConnState) error {
	cfg := state.ResolverState.Attributes.Value(attrKey)
	if cfg == nil {
		logB.Error("UpdateClientConnState invoked with empty config.")
		return balancer.ErrBadResolverState
	}

	ds, ok := cfg.(*control.DataServers)
	if !ok {
		logB.Error("UpdateClientConnState invoked with bad config.")
		return balancer.ErrBadResolverState
	}

	addresses := map[string]bool{}
	serverAddress := map[uint64]string{}
	for _, server := range ds.Servers {
		addresses[server.Address] = true
		serverAddress[server.Id] = server.Address
	}

	// close connections that are no longer necessary
	for address, conn := range b.subConns {
		if addresses[address] {
			delete(b.subConns, address)
			delete(b.subConnsState, address)
			conn.Shutdown()
			logB.Debug("Connection closed.", "address", address)
		}
	}

	// remove old servers
	for serverID := range b.serverAddress {
		if _, ok := serverAddress[serverID]; !ok {
			delete(b.serverAddress, serverID)
		}
	}

	// open connections
	for address := range addresses {
		if b.subConns[address] != nil {
			continue
		}

		log := logB.With("address", address)
		opts := balancer.NewSubConnOptions{
			StateListener: b.makeStateListener(address, log),
		}

		subConn, err := b.clientConn.NewSubConn([]resolver.Address{{Addr: address}}, opts)
		if err != nil {
			log.WithError(err).Error("NewSubConn failed")
			return balancer.ErrBadResolverState
		}

		b.subConns[address] = subConn
		subConn.Connect()
		log.Debug("Connection created")
	}

	// arrange conns per partitions for easy lookup during pick
	partitions := map[uint32]partition{}

	getConn := func(serverID uint64) balancer.SubConn {
		addr := serverAddress[serverID]
		return b.subConns[addr]
	}

	for partID := range ds.PartitionCount {
		part := ds.Partitions[partID]

		read := make([]balancer.SubConn, len(part.ReadServerIds))
		for i, serverID := range part.ReadServerIds {
			read[i] = getConn(serverID)
		}

		partitions[partID] = partition{
			readServerIDs: part.ReadServerIds,
			readConns:     read,
			writeServerID: part.WriteServerId,
			writeConn:     getConn(part.WriteServerId),
		}
	}

	b.picker = &picker{
		balancer:   b,
		partitions: partitions,
	}

	return nil
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

		if _, ok := b.subConns[address]; !ok {
			return
		}

		b.subConnsState[address] = state.ConnectivityState
		lbState := b.getLBConnectivityState(log)

		b.clientConn.UpdateState(balancer.State{
			ConnectivityState: lbState,
			Picker:            b.picker,
		})
	}
}

func (b *balancerImpl) getLBConnectivityState(log logging.LoggerNoContext) connectivity.State {
	if len(b.subConns) == 0 {
		return connectivity.Idle
	}

	// Some heuristics/feels-good-like logic follow below as the LB needs to
	// strike a balance between waiting for all connections to be ready and
	// start routing requests as soon as possible to avoid unavailability.
	// Some ideas, might review later:
	//  - check the availability of each partititon: a partition is available
	//    if at least one read server is ready along with the write server,
	//  - if at least 80% of partitions are available declare the LB ready,
	//  - else, if above 20% connections are in transient failure return failure,
	//  - else, return connecting state.

	total := len(b.partitions)
	available := 0

	isReady := func(serverID uint64) bool {
		addr := b.serverAddress[serverID]
		state := b.subConnsState[addr]
		return state == connectivity.Ready
	}

LOOP_PART:
	for _, part := range b.partitions {
		if !isReady(part.writeServerID) {
			continue
		}

		for _, id := range part.readServerIDs {
			if isReady(id) {
				available++
				continue LOOP_PART
			}
		}
	}

	// 80% available partitions?
	if float64(available)/float64(total) >= .8 {
		log.Debugf("LB Ready: %d/%d partitions available.", available, total)
		return connectivity.Ready
	}

	failure := 0
	for _, state := range b.subConnsState {
		if state == connectivity.TransientFailure {
			failure++
		}
	}

	// 20% failing connections?
	if float64(failure)/float64(len(b.subConns)) >= .2 {
		log.Debugf("LB TransientFailure: %d/%d partitions available, %d/%d failing connections.", available, total, failure, len(b.subConns))
		return connectivity.TransientFailure
	}

	log.Debugf("LB Connecting: %d/%d partitions available, %d/%d failing connections.", available, total, failure, len(b.subConns))
	return connectivity.Connecting
}

func (b *balancerImpl) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	logB.Warn("Unexpected call to deprecated method UpdateSubConnState")
}

func (b *balancerImpl) ResolverError(err error) {}

func (b *balancerImpl) Close() {}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	// TODO: partition routing logic: hashing, read/write, read from replica, etc.
	isRead := true
	partID := uint32(0)

	part := p.partitions[partID]

	var conn balancer.SubConn

	if isRead {
		conn = part.readConns[0] // TODO: check conn state; round robin, etc
	} else {
		conn = part.writeConn
	}

	// TODO: balancer.ErrNoSubConnAvailable if partition is offline

	return balancer.PickResult{
		SubConn: conn,
		Done:    p.rpcDone,
	}, nil
}

func (p *picker) rpcDone(d balancer.DoneInfo) {
	if d.Err == errors.Unavailable {
		// the resolver will throttle the ResolveNow calls
		p.balancer.clientConn.ResolveNow(resolver.ResolveNowOptions{})
	}
}
