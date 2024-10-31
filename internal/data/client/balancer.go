package client

import (
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/routing"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

var (
	errPreferredUnavailable = status.Error(codes.Unavailable, "Preferred server is unavailable")
	logLB                   = logging.New("data_balancer").NoContext()
)

type balancerBuilder struct{}

type balancerImpl struct {
	conn       *routing.ClientConn
	etag       string
	partitions map[uint32]*partition
	addrs      []string
	picker     *picker
}

type picker struct {
	conn       *routing.ClientConn
	partitions map[uint32]*partition
}

type partition struct {
	etag         string
	id           uint32
	leaderId     uint64 // server_id
	leader       string // address
	replicaCount int
	replicaSlice []string          // []address
	replicaMap   map[uint64]string // map[server_id]address
	replicaRR    atomic.Int32
}

type balancerCfg struct {
	dataServers       *control.DataServers
	reconnectInterval time.Duration
}

func (b *balancerBuilder) Build(clientConn balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &balancerImpl{
		conn: routing.NewClientConn(clientConn),
	}
}

func (b *balancerBuilder) Name() string {
	return serviceconfig.LBNameScoutData
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
	}

	if b.etag == cfg.dataServers.ETag {
		return nil
	}

	logLB.Debug("UpdateClientConnState with new config.", "etag", cfg.dataServers.ETag)

	b.conn.SetLog(logLB)
	b.conn.SetReconnectInterval(cfg.reconnectInterval)
	b.conn.SetStateChangedCallback(b.updateState)

	addrs := make([]string, 0, len(cfg.dataServers.Servers))
	for _, server := range cfg.dataServers.Servers {
		addrs = append(addrs, server.Address)
	}

	b.updatePartitions(cfg.dataServers)
	b.addrs = addrs
	b.etag = cfg.dataServers.ETag

	return b.conn.Connect(addrs...)
}

func (b *balancerImpl) updateState() {
	b.conn.UpdateState(balancer.State{
		ConnectivityState: b.getLBConnectivityState(),
		Picker:            b.picker,
	})
}

func (b *balancerImpl) updatePartitions(ds *control.DataServers) {
	// arrange by partitions for easy lookup during pick
	partitions := map[uint32]*partition{}

	for pid := range ds.PartitionCount {
		part := ds.Partitions[pid]

		if b.partitions != nil {
			if oldPart := b.partitions[pid]; oldPart.etag == part.ETag {
				partitions[pid] = oldPart
				continue
			}
		}

		var leader string
		if server, ok := ds.Servers[part.LeaderServerId]; ok {
			leader = server.Address
		}

		replicaSlice := make([]string, len(part.ReplicaServerIds))
		replicaMap := map[uint64]string{}
		for i, serverId := range part.ReplicaServerIds {
			addr := ds.Servers[serverId].Address
			replicaSlice[i] = addr
			replicaMap[serverId] = addr
		}

		partitions[pid] = &partition{
			etag:         part.ETag,
			id:           pid,
			leaderId:     part.LeaderServerId,
			leader:       leader,
			replicaCount: len(replicaSlice),
			replicaSlice: utils.ShuffleSlice(replicaSlice),
			replicaMap:   replicaMap,
		}
	}

	b.partitions = partitions
	b.picker = &picker{b.conn, partitions}
}

func (b *balancerImpl) getLBConnectivityState() connectivity.State {
	if b.conn.Count() == 0 {
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
		if b.conn.IsReady(part.leader) {
			available++
		}
	}

	// 80% available partitions?
	if float64(available)/float64(total) >= .8 {
		logLB.Debugf("LB Ready: %d/%d partitions available.", available, total)
		return connectivity.Ready
	}

	failure := 0
	for _, addr := range b.addrs {
		if b.conn.State(addr) == connectivity.TransientFailure {
			failure++
		}
	}

	// 20% failing connections?
	if float64(failure)/float64(b.conn.Count()) >= .2 {
		logLB.Debugf("LB TransientFailure: %d/%d partitions available, %d/%d failing connections.", available, total, failure, b.conn.Count())
		return connectivity.TransientFailure
	}

	logLB.Debugf("LB Connecting: %d/%d partitions available, %d/%d failing connections.", available, total, failure, b.conn.Count())
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
	}

	part, ok := p.partitions[routing.partitionID]
	if !ok {
		return balancer.PickResult{}, status.Error(codes.Internal, "Invalid partition ID.")
	}

	preferred, hasPreferred := getPreferredServer(info.Ctx)

	unavailableErr := func() error {
		if hasPreferred && preferred.enforce {
			return errPreferredUnavailable
		}
		return balancer.ErrNoSubConnAvailable
	}

	if !routing.snapshotRead {
		switch {
		case part.leader == "":
			logLB.Debug("Leader connection not available.", "partition", part.id)
			return balancer.PickResult{}, unavailableErr()
		case !p.conn.IsReady(part.leader):
			logLB.Debug("Leader connection not ready.", "partition", part.id)
			return balancer.PickResult{}, unavailableErr()
		case hasPreferred && preferred.enforce && preferred.serverID != part.leaderId:
			return balancer.PickResult{}, errPreferredUnavailable
		}

		conn := p.conn.SubConn(part.leader)
		if conn == nil {
			return balancer.PickResult{}, unavailableErr()
		}

		return balancer.PickResult{
			SubConn: conn,
			Done:    p.rpcDone,
		}, nil
	}

	switch {
	case part.replicaCount == 0:
		return balancer.PickResult{}, unavailableErr()
	case hasPreferred:
		addr := part.replicaMap[preferred.serverID]
		conn := p.conn.SubConn(addr)

		switch {
		case conn != nil:
			return balancer.PickResult{
				SubConn: conn,
				Done:    p.rpcDone,
			}, nil
		case preferred.enforce:
			return balancer.PickResult{}, errPreferredUnavailable
		default:
			// continue to round-robin server selection below
		}
	}

	curr := int(part.replicaRR.Add(1)) % part.replicaCount

	for range part.replicaCount {
		addr := part.replicaSlice[curr]
		conn := p.conn.SubConn(addr)

		if conn == nil || !p.conn.IsReady(addr) {
			curr = (curr + 1) % part.replicaCount
			continue
		}

		return balancer.PickResult{
			SubConn: conn,
			Done:    p.rpcDone,
		}, nil
	}

	logLB.Debug("Read connections not ready.", "partition", part.id)
	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}

func (p *picker) rpcDone(d balancer.DoneInfo) {
	if errors.IsAny(d.Err, errors.Unavailable, errors.NotLeader) {
		// the resolver will throttle the ResolveNow calls
		p.conn.ResolveNow(resolver.ResolveNowOptions{})
	}
}
