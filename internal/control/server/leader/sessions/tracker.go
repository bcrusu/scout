package sessions

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bcrusu/graph/internal/api"
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/config"
	"github.com/bcrusu/graph/internal/control/server/convert"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/events"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"github.com/bcrusu/graph/internal/utils"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

const (
	sessionSendBufferChSize = 32
	writeServersInterval    = 5 * time.Second
)

var (
	logS                                       = logging.WithComponent("session_tracker")
	_                          utils.Lifecycle = (*Tracker)(nil)
	recvBurst                                  = 5
	recvLimit                                  = rate.Limit(float64(recvBurst) / float64(time.Second.Seconds()))
	recvMaxOffenses                            = 16 // after this the session will be closed
	updateServerListDebounce                   = 200 * time.Millisecond
	updateServerConfigDebounce                 = 200 * time.Millisecond
)

type Tracker struct {
	config                config.Service
	store                 storage.Store
	startSessionCh        chan startSession
	sessionCh             chan sessionMessage
	cancelFunc            context.CancelFunc
	dataServiceConfigJson string
	apiServiceConfigJson  string
	dataServers           atomic.Pointer[control.DataServers]
	dataServersVersion    atomic.Uint64
	apiServers            atomic.Pointer[control.ApiServers]
	apiServersVersion     atomic.Uint64
}

type session struct {
	id            sessionID
	serverID      serverID
	serverType    control.ServerType
	serverAddress string
	createdAt     time.Time
	lastSeen      time.Time
	sendBufferCh  chan *control.SessionOut
	ctx           context.Context
	log           logging.Logger
	waitCh        chan error
	dsConfig      *control.DataServerConfig // only for data servers
	asConfig      *control.ApiServerConfig  // only for api servers
	recvLimiter   *rate.Limiter             // dedicated for recv loop
	recvOffenses  int                       // dedicated for recv loop
}

type partitionState struct {
	id         partitionID
	hasLeader  bool
	leaderID   serverID // current leader server id
	leaderTerm uint64   // current leader raft term
}

// having ID types defined enforces proper discipline in handling diff ID types
type sessionID uint64
type serverID uint64
type partitionID uint32

type sessionStream = grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]
type sessions map[sessionID]*session
type sessionsByServer map[serverID]*session
type partitionStates map[partitionID]*partitionState

func NewTracker(config config.Service, store storage.Store) *Tracker {
	return &Tracker{
		config:                config,
		store:                 store,
		startSessionCh:        make(chan startSession),
		sessionCh:             make(chan sessionMessage, 1),
		dataServiceConfigJson: config.DataClient.GetServiceConfigJson(serviceconfig.LBNameGraphData, data.Service_ServiceDesc),
		apiServiceConfigJson:  config.ApiClient.GetServiceConfigJson(serviceconfig.LBNameGraphApi, api.Service_ServiceDesc),
	}
}

func (t *Tracker) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(t.mainLoop)
	t.cancelFunc = cancelFunc

	go mainLoop(ctx)
	return nil
}

func (t *Tracker) Stop() {
	t.cancelFunc()
}

func (t *Tracker) NewSession(stream sessionStream) error {
	in, err := stream.Recv()
	if err != nil {
		return errors.Wrap(err, "new session failed before hello")
	}

	if in == nil || in.Payload == nil {
		return errors.InvalidRequest
	}

	payload, ok := in.Payload.(*control.SessionIn_Hello)
	if !ok {
		return errors.ValidationError{Message: "Please don't be rude."}
	} else if err := payload.Hello.Validate(); err != nil {
		return errors.InvalidRequest
	} else if payload.Hello.ClusterName != t.store.ClusterName() {
		return errors.PermissionDenied
	}

	cmd := startSession{
		stream:        stream,
		serverID:      serverID(payload.Hello.ServerId),
		serverAddress: payload.Hello.Address,
		waitCh:        make(chan error, 1),
	}

	t.startSessionCh <- cmd

	// Wait until we get the signal from the main loop below. Once
	// this method returns, gRPC will close the stream resulting in both
	// send and receive loops to end.
	return <-cmd.waitCh
}

func (t *Tracker) mainLoop(ctx context.Context) {
	serversSub := events.Subscribe[*storage.Servers]()
	partitionsSub := events.Subscribe[*storage.Partitions]()
	writeServersTicker := time.NewTicker(writeServersInterval)
	defer serversSub.Unsubscribe()
	defer partitionsSub.Unsubscribe()
	defer writeServersTicker.Stop()

	dsUpdateCh, dsUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerListDebounce, 1)
	asUpdateCh, asUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerListDebounce, 1)
	dsConfigsUpdateCh, dsConfigsUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerConfigDebounce, 1)
	asConfigsUpdateCh, asConfigsUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerConfigDebounce, 1)

	servers := t.store.Servers()
	partitions := t.store.Partitions()
	dsConfigs := makeDataServerConfigs(servers, partitions, dsConfigs{})
	asConfigs := makeApiServerConfigs(servers, asConfigs{})

	sessionCounter := sessionID(0)
	sessionsById := sessions{}
	sessionsByServer := sessionsByServer{}
	partitionStates := t.initPartitionStates()
	runningLoops := 0

	t.updateDataServerList(servers, partitions, sessionsByServer, partitionStates)
	t.updateApiServerList(servers, sessionsByServer)

	closeSession := func(id sessionID, err error) {
		if sess, ok := sessionsById[id]; ok {
			delete(sessionsById, sess.id)
			delete(sessionsByServer, sess.serverID)
			sess.log.Debug(sess.ctx, "Session closed.")
			sess.waitCh <- err // signals the waiting NewSession call above
		}
	}

	for {
		select {
		case x := <-t.startSessionCh:
			server := servers.ByID(uint64(x.serverID))
			if server == nil {
				logS.Warnf(ctx, "Session hello for unknwon server %d at %s.", x.serverID, x.serverAddress)
				x.waitCh <- errors.NotRegistered
				continue
			}
			if server.Type == storage.ServerType_Control {
				logS.Warnf(ctx, "Control server %d at %s trying to start session.", x.serverID, x.serverAddress)
				x.waitCh <- errors.PermissionDenied
				continue
			}

			now := time.Now().UTC()
			sessionCounter++
			needsUpdate := true

			new := &session{
				id:            sessionCounter,
				serverID:      x.serverID,
				serverType:    convert.FromServerType(server.Type),
				serverAddress: x.serverAddress,
				createdAt:     now,
				lastSeen:      now,
				sendBufferCh:  make(chan *control.SessionOut, sessionSendBufferChSize),
				ctx:           x.stream.Context(),
				waitCh:        x.waitCh,
				dsConfig:      dsConfigs[x.serverID],
				asConfig:      asConfigs[x.serverID],
				recvLimiter:   rate.NewLimiter(recvLimit, recvBurst),
			}

			new.log = server.AddToLog(logS).With("session_id", new.id, "address", new.serverAddress)

			if old := sessionsByServer[x.serverID]; old != nil {
				new.log.Debugf(ctx, "Closing old session %d created at %v.", old.id, old.createdAt)
				needsUpdate = old.serverAddress != new.serverAddress
				closeSession(old.id, nil)
			}

			sessionsById[new.id] = new
			sessionsByServer[new.serverID] = new

			go t.sessionSendLoop(new, x.stream)
			go t.sessionRecvLoop(new, x.stream)
			runningLoops += 2

			new.log.Info(ctx, "Started new session.")

			if !needsUpdate {
				continue
			}

			switch new.serverType {
			case control.ServerType_Data:
				dsUpdateCh <- true
			case control.ServerType_Api:
				asUpdateCh <- true
			}
		case msg := <-t.sessionCh:
			if x, ok := msg.(sessionLoopDone); ok {
				runningLoops--
				closeSession(x.id, x.err)
				continue
			}

			sess := sessionsById[msg.ID()]
			if sess == nil {
				continue
			}

			switch x := msg.(type) {
			case sessionReceived:
				sess.lastSeen = time.Now().UTC()
			case sessionPartStatus:
				for id, term := range x.leader {
					part := partitionStates[id]
					part.hasLeader = true
					if term > part.leaderTerm {
						part.leaderID = sess.serverID
						part.leaderTerm = term
					}
				}
				dsUpdateCh <- true
			default:
				logS.Errorf(ctx, "Unknown session message type %T", msg)
			}
		case servers = <-serversSub.Items():
			// close sessions for removed servers
			for serverID, sess := range sessionsByServer {
				if x := servers.ByID(uint64(serverID)); x == nil {
					closeSession(sess.id, errors.NotRegistered)
				}
			}

			dsUpdateCh <- true
			asUpdateCh <- true
			dsConfigsUpdateCh <- true
			asConfigsUpdateCh <- true
		case partitions = <-partitionsSub.Items():
			dsUpdateCh <- true
			dsConfigsUpdateCh <- true
		case <-writeServersTicker.C:
			t.writeUpdateServers(ctx, servers, sessionsByServer)
		case <-dsUpdateChDb:
			t.updateDataServerList(servers, partitions, sessionsByServer, partitionStates)
		case <-asUpdateChDb:
			t.updateApiServerList(servers, sessionsByServer)
		case <-dsConfigsUpdateChDb:
			dsConfigs = t.updateDataServerConfigs(servers, partitions, sessionsByServer, dsConfigs)
		case <-asConfigsUpdateChDb:
			asConfigs = t.updateApiServerConfigs(servers, sessionsByServer, asConfigs)
		case <-ctx.Done():
			goto SHUTDOWN
		}
	}

SHUTDOWN:
	for id := range sessionsById {
		closeSession(id, nil)
	}

	// drain
	for {
		select {
		case cmd := <-t.startSessionCh:
			// reject new sessions
			cmd.waitCh <- errors.Unavailable
		case msg := <-t.sessionCh:
			if _, ok := msg.(sessionLoopDone); !ok {
				continue
			}

			runningLoops--
			if runningLoops == 0 {
				return
			}
		}
	}
}

func (t *Tracker) initPartitionStates() partitionStates {
	result := partitionStates{}
	for i := range t.store.PartitionCount() {
		id := partitionID(i)
		result[id] = &partitionState{
			id:        id,
			hasLeader: false,
		}
	}
	return result
}

func (t *Tracker) updateDataServerList(servers *storage.Servers, partitions *storage.Partitions, sessions sessionsByServer, partitionStates partitionStates) {
	ds := &control.DataServers{
		Version:           t.dataServersVersion.Add(1),
		Servers:           map[uint64]*control.DataServers_Server{},
		Partitions:        map[uint32]*control.DataServers_Partition{},
		PartitionCount:    t.store.PartitionCount(),
		ServiceConfigJson: t.dataServiceConfigJson,
	}

	for id, server := range servers.ByType(storage.ServerType_Data) {
		ds.Servers[id] = &control.DataServers_Server{
			Id:      id,
			Address: server.LastAddress,
		}

		if sess := sessions[serverID(id)]; sess != nil {
			ds.Servers[id].Address = sess.serverAddress
		}
	}

	for id, part := range partitions.Items {
		readServerIDs := make([]uint64, len(part.Members))
		for i, member := range part.Members {
			readServerIDs[i] = member.ServerId
		}

		ds.Partitions[id] = &control.DataServers_Partition{
			Id:            id,
			ReadServerIds: readServerIDs,
		}

		if state := partitionStates[partitionID(id)]; state.hasLeader {
			ds.Partitions[id].WriteServerId = uint64(state.leaderID)
		}
	}

	t.dataServers.Store(ds)
	sessions.trySendAll(newSessionOut(ds))
}

func (t *Tracker) updateApiServerList(servers *storage.Servers, sessions sessionsByServer) {
	as := &control.ApiServers{
		Version:           t.apiServersVersion.Add(1),
		Servers:           map[uint64]*control.ApiServers_Server{},
		ServiceConfigJson: t.apiServiceConfigJson,
	}

	for id, server := range servers.ByType(storage.ServerType_Api) {
		as.Servers[id] = &control.ApiServers_Server{
			Id:      id,
			Address: server.LastAddress,
		}

		if sess := sessions[serverID(id)]; sess != nil {
			as.Servers[id].Address = sess.serverAddress
		}
	}

	t.apiServers.Store(as)

	out := newSessionOut(as)
	for _, sess := range sessions {
		if sess.serverType == control.ServerType_Api {
			sess.trySend(out)
		}
	}
}

func (t *Tracker) updateDataServerConfigs(servers *storage.Servers, partitions *storage.Partitions, sessions sessionsByServer, oldConfigs dsConfigs) dsConfigs {
	newConfigs := makeDataServerConfigs(servers, partitions, oldConfigs)

	for _, sess := range sessions {
		if sess.serverType != control.ServerType_Data {
			continue
		}

		config := newConfigs[sess.serverID]
		if config.Version != sess.dsConfig.Version {
			sess.dsConfig = config
			sess.trySend(newSessionOut(config))
		}
	}

	return newConfigs
}

func (t *Tracker) updateApiServerConfigs(servers *storage.Servers, sessions sessionsByServer, oldConfigs asConfigs) asConfigs {
	newConfigs := makeApiServerConfigs(servers, oldConfigs)

	for _, sess := range sessions {
		if sess.serverType != control.ServerType_Api {
			continue
		}

		config := newConfigs[sess.serverID]
		if config.Version != sess.asConfig.Version {
			sess.asConfig = config
			sess.trySend(newSessionOut(config))
		}
	}

	return newConfigs
}
