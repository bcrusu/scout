package client

import (
	"sync/atomic"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

var (
	logLB = logging.WithComponent("data_balancer").NoContext()
)

type balancerBuilder struct{}

type balancerImpl struct {
	clientConn balancer.ClientConn
	etag       string
	subConns   map[uint64]*subConn   // map[server_id]subConn
	partitions map[uint32]*partition // map[part_id]part
	picker     *picker
}

type subConn struct {
	balancer.SubConn
	serverID uint64
	address  string
	state    connectivity.State
}

type picker struct {
	balancer       *balancerImpl
	partitionCount uint32
	partitions     map[uint32]*partition // map[part_id]part
}

type partition struct {
	id         uint32
	leader     *subConn
	replicas   []*subConn
	replicasRR atomic.Int32
}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &balancerImpl{
		clientConn: clientConn,
		subConns:   map[uint64]*subConn{},
	}
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameGraphData
}

func (b *balancerImpl) UpdateClientConnState(state balancer.ClientConnState) error {
	cfg := state.ResolverState.Attributes.Value(attrKey)
	if cfg == nil {
		logLB.Error("UpdateClientConnState invoked with empty config.")
		return balancer.ErrBadResolverState
	}

	ds, ok := cfg.(*control.DataServers)
	if !ok {
		logLB.Error("UpdateClientConnState invoked with bad config.")
		return balancer.ErrBadResolverState
	}

	if b.etag == ds.ETag {
		return nil
	}

	// close connections that are no longer necessary
	for serverID, conn := range b.subConns {
		if _, ok := ds.Servers[serverID]; !ok {
			delete(b.subConns, serverID)
			conn.Shutdown()
			logLB.Debug("Connection closed.", "server_id", serverID, "address", conn.address)
		}
	}

	// open connections
	for serverID, server := range ds.Servers {
		if _, ok := b.subConns[serverID]; ok {
			continue
		}

		log := logLB.With("server_id", serverID, "address", server.Address)
		opts := balancer.NewSubConnOptions{
			StateListener: b.makeStateListener(serverID, server.Address, log),
		}

		newSubConn, err := b.clientConn.NewSubConn([]resolver.Address{{Addr: server.Address}}, opts)
		if err != nil {
			log.WithError(err).Error("NewSubConn failed")
			return balancer.ErrBadResolverState
		}

		conn := &subConn{
			serverID: serverID,
			address:  server.Address,
			SubConn:  newSubConn,
			state:    connectivity.Idle,
		}

		b.subConns[serverID] = conn
		conn.Connect()
		log.Debug("Connection created")
	}

	b.etag = ds.ETag
	b.picker = b.makePicker(ds)
	b.updateState()
	return nil
}

func (b *balancerImpl) updateState() {
	b.clientConn.UpdateState(balancer.State{
		ConnectivityState: b.getLBConnectivityState(),
		Picker:            b.picker,
	})
}

func (b *balancerImpl) makeStateListener(serverID uint64, address string, log logging.LoggerNoContext) func(balancer.SubConnState) {
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

		// if server was removed or its address changed:
		if conn, ok := b.subConns[serverID]; !ok || conn.address != address {
			return
		}

		b.subConns[serverID].state = state.ConnectivityState
		lbState := b.getLBConnectivityState()

		b.clientConn.UpdateState(balancer.State{
			ConnectivityState: lbState,
			Picker:            b.picker,
		})
	}
}

func (b *balancerImpl) makePicker(ds *control.DataServers) *picker {
	// arrange conns per partitions for easy lookup during pick
	partitions := map[uint32]*partition{}

	for partID := range ds.PartitionCount {
		part := ds.Partitions[partID]

		var leader *subConn
		if part.LeaderServerId != 0 {
			leader = b.subConns[part.LeaderServerId]
		}

		replicas := make([]*subConn, len(part.ReplicaServerIds))
		for i, serverID := range part.ReplicaServerIds {
			replicas[i] = b.subConns[serverID]
		}

		partitions[partID] = &partition{
			id:       partID,
			leader:   leader,
			replicas: utils.ShuffleSlice(replicas),
		}
	}

	return &picker{
		balancer:       b,
		partitionCount: ds.PartitionCount,
		partitions:     partitions,
	}
}

func (b *balancerImpl) getLBConnectivityState() connectivity.State {
	if len(b.subConns) == 0 {
		return connectivity.Idle
	}

	// Some heuristics/feels-good-like logic follow below as the LB needs to
	// strike a balance between waiting for all connections to be ready and
	// start routing requests as soon as possible to avoid unavailability.
	// Some ideas, might review later:
	//  - check the availability of each partititon: a partition is available
	//    if the connection to the leader is ready,
	//  - if at least 80% of partitions are available declare the LB ready,
	//  - else, if above 20% connections are in transient failure return failure,
	//  - else, return connecting state.
	total := len(b.partitions)
	available := 0

	for _, part := range b.partitions {
		if part.leader.state == connectivity.Ready {
			available++
		}
	}

	// 80% available partitions?
	if float64(available)/float64(total) >= .8 {
		logLB.Debugf("LB Ready: %d/%d partitions available.", available, total)
		return connectivity.Ready
	}

	failure := 0
	for _, conn := range b.subConns {
		if conn.state == connectivity.TransientFailure {
			failure++
		}
	}

	// 20% failing connections?
	if float64(failure)/float64(len(b.subConns)) >= .2 {
		logLB.Debugf("LB TransientFailure: %d/%d partitions available, %d/%d failing connections.", available, total, failure, len(b.subConns))
		return connectivity.TransientFailure
	}

	logLB.Debugf("LB Connecting: %d/%d partitions available, %d/%d failing connections.", available, total, failure, len(b.subConns))
	return connectivity.Connecting
}

func (b *balancerImpl) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	logLB.Warn("Unexpected call to deprecated method UpdateSubConnState")
}

func (b *balancerImpl) ResolverError(err error) {}

func (b *balancerImpl) Close() {}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	routing, ok := getRouting(info.Ctx)
	if !ok {
		return balancer.PickResult{}, status.Error(codes.Internal, "Missing routing info.")
	} else if routing.partitionID >= p.partitionCount {
		return balancer.PickResult{}, status.Error(codes.Internal, "Invalid partition ID.")
	}

	part := p.partitions[routing.partitionID]

	if !routing.replicaRead {
		if part.leader == nil {
			logLB.Debug("Leader connection not available.", "partition_id", part.id)
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}

		if part.leader.state != connectivity.Ready {
			logLB.Debug("Leader connection not ready.", "partition_id", part.id)
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}

		return balancer.PickResult{
			SubConn: part.leader.SubConn,
			Done:    p.rpcDone,
		}, nil
	}

	if len(part.replicas) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	curr := int(part.replicasRR.Add(1)) % len(part.replicas)

	for range len(part.replicas) {
		conn := part.replicas[curr]

		if conn.state != connectivity.Ready {
			curr = (curr + 1) % len(part.replicas)
			continue
		}

		return balancer.PickResult{
			SubConn: conn.SubConn,
			Done:    p.rpcDone,
		}, nil
	}

	logLB.Debug("Read connections not ready.", "partition_id", part.id)
	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}

func (p *picker) rpcDone(d balancer.DoneInfo) {
	if errors.IsAny(d.Err, errors.Unavailable, errors.NotLeader) {
		// the resolver will throttle the ResolveNow calls
		p.balancer.clientConn.ResolveNow(resolver.ResolveNowOptions{})
	}
}
