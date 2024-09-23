package sessions

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/bcrusu/graph/internal/api"
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/config"
	"github.com/bcrusu/graph/internal/control/server/convert"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/eventbus"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"github.com/bcrusu/graph/internal/utils"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

const (
	sessionSendBufferChSize   = 32
	writeLatestStatusInterval = 5 * time.Second
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
	apiServers            atomic.Pointer[control.ApiServers]
}

type session struct {
	id            sessionID
	serverID      uint64
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

type sessionStream = grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]
type sessionID uint64
type sessions map[sessionID]*session
type sessionsByServer map[uint64]*session

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
		serverID:      payload.Hello.ServerId,
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
	serversSub := eventbus.Subscribe[*storage.Servers]()
	partitionsSub := eventbus.Subscribe[*storage.Partitions]()
	writeLatestStatusTicker := time.NewTicker(writeLatestStatusInterval)
	defer serversSub.Unsubscribe()
	defer partitionsSub.Unsubscribe()
	defer writeLatestStatusTicker.Stop()

	dsUpdateCh, dsUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerListDebounce, 1)
	asUpdateCh, asUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerListDebounce, 1)
	dsConfigsUpdateCh, dsConfigsUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerConfigDebounce, 1)
	asConfigsUpdateCh, asConfigsUpdateChDb := utils.MakeDebounceChan[bool](ctx, updateServerConfigDebounce, 1)

	servers := t.store.Servers()
	partitions := t.store.Partitions()
	status := newStatusTracker(servers, partitions)
	dsConfigs := makeDataServerConfigs(servers, partitions)
	asConfigs := makeApiServerConfigs(servers)

	sessionCounter := sessionID(0)
	sessionsById := sessions{}
	sessionsByServer := sessionsByServer{}
	runningLoops := 0

	t.updateDataServerList(servers, partitions, status)
	t.updateApiServerList(servers, status)

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
			server := servers.ByID(x.serverID)
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
			status.recordNewSession(new)

			go t.sessionSendLoop(new, x.stream)
			go t.sessionRecvLoop(new, x.stream)
			runningLoops += 2

			new.log.Info(ctx, "Started new session.")

			if needsUpdate {
				switch new.serverType {
				case control.ServerType_Data:
					dsUpdateCh <- true
				case control.ServerType_Api:
					asUpdateCh <- true
				}
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
				status.recordSessionReceived(sess)
			case dataServerStatus:
				if status.recordReplicaStatus(x.status.Replicas) {
					dsUpdateCh <- true
				}
			default:
				logS.Errorf(ctx, "Unknown session message type %T", msg)
			}
		case newServers := <-serversSub.Items():
			if newServers.ItemsVersion == servers.ItemsVersion {
				continue
			}

			// close sessions for removed servers
			for serverID, sess := range sessionsByServer {
				if newServers.ByID(serverID) == nil {
					closeSession(sess.id, errors.NotRegistered)
				}
			}

			servers = newServers
			dsUpdateCh <- true
			asUpdateCh <- true
			dsConfigsUpdateCh <- true
			asConfigsUpdateCh <- true
		case newPartitions := <-partitionsSub.Items():
			if newPartitions.ItemsVersion == partitions.ItemsVersion {
				continue
			}

			partitions = newPartitions
			dsUpdateCh <- true
			dsConfigsUpdateCh <- true
		case <-writeLatestStatusTicker.C:
			t.writeLatestStatus(ctx, status)
		case <-dsUpdateChDb:
			if t.updateDataServerList(servers, partitions, status) {
				out := newSessionOut(t.dataServers.Load())
				sessionsById.trySendAll(out)
			}
		case <-asUpdateChDb:
			if t.updateApiServerList(servers, status) {
				out := newSessionOut(t.apiServers.Load())
				sessionsById.trySendServerType(out, control.ServerType_Api)
			}
		case <-dsConfigsUpdateChDb:
			dsConfigs = t.updateDataServerConfigs(servers, partitions, sessionsByServer)
		case <-asConfigsUpdateChDb:
			asConfigs = t.updateApiServerConfigs(servers, sessionsByServer)
		case <-ctx.Done():
			goto SHUTDOWN
		}
	}

SHUTDOWN:
	for id := range sessionsById {
		closeSession(id, nil)
	}

	t.writeLatestStatus(ctx, status)

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

func (t *Tracker) updateDataServerList(servers *storage.Servers, partitions *storage.Partitions, status *statusTracker) bool {
	new := &control.DataServers{
		Servers:           map[uint64]*control.DataServers_Server{},
		Partitions:        map[uint32]*control.DataServers_Partition{},
		PartitionCount:    t.store.PartitionCount(),
		ServiceConfigJson: t.dataServiceConfigJson,
	}

	for id := range servers.ByType(storage.ServerType_Data) {
		new.Servers[id] = &control.DataServers_Server{
			Id:      id,
			Address: status.getServerLastAddress(id),
		}
	}

	for id, part := range partitions.Items {
		writeServerId := uint64(0)
		readServerIDs := make([]uint64, 0, len(part.Replicas))

		for _, replica := range part.Replicas {
			if replica.State != storage.Partition_Voter && replica.State != storage.Partition_NonVoter {
				continue
			}

			readServerIDs = append(readServerIDs, replica.ServerId)
		}
		slices.Sort(readServerIDs)

		if leader := partitions.Status[id].Leader; leader != "" {
			writeServerId = part.Replicas[leader].ServerId
		}

		new.Partitions[id] = &control.DataServers_Partition{
			Id:            id,
			WriteServerId: writeServerId,
			ReadServerIds: readServerIDs,
		}
	}

	etags := make([]string, 0, len(new.Partitions)+len(new.Servers))
	for id, server := range new.Servers {
		etags = append(etags, fmt.Sprintf("srv %d:%s", id, server.Address))
	}
	for id, part := range new.Partitions {
		etags = append(etags, fmt.Sprintf("part %d:%d:%v", id, part.WriteServerId, part.ReadServerIds))
	}

	new.ETag = makeETag(etags...)
	if old := t.dataServers.Load(); old != nil && new.ETag == old.ETag {
		return false
	}

	t.dataServers.Store(new)
	return true
}

func (t *Tracker) updateApiServerList(servers *storage.Servers, status *statusTracker) bool {
	new := &control.ApiServers{
		Servers:           map[uint64]*control.ApiServers_Server{},
		ServiceConfigJson: t.apiServiceConfigJson,
	}

	for id := range servers.ByType(storage.ServerType_Api) {
		new.Servers[id] = &control.ApiServers_Server{
			Id:      id,
			Address: status.getServerLastAddress(id),
		}
	}

	etags := make([]string, 0, len(new.Servers))
	for id, server := range new.Servers {
		etags = append(etags, fmt.Sprintf("srv %d:%s", id, server.Address))
	}

	new.ETag = makeETag(etags...)
	if old := t.dataServers.Load(); old != nil && new.ETag == old.ETag {
		return false
	}

	t.apiServers.Store(new)
	return true
}

func (t *Tracker) updateDataServerConfigs(servers *storage.Servers, partitions *storage.Partitions, sessions sessionsByServer) dsConfigs {
	new := makeDataServerConfigs(servers, partitions)

	for _, sess := range sessions {
		if sess.serverType != control.ServerType_Data {
			continue
		}

		config := new[sess.serverID]
		if config.ETag != sess.dsConfig.ETag {
			sess.dsConfig = config
			sess.trySend(newSessionOut(config))
		}
	}

	return new
}

func (t *Tracker) updateApiServerConfigs(servers *storage.Servers, sessions sessionsByServer) asConfigs {
	new := makeApiServerConfigs(servers)

	for _, sess := range sessions {
		if sess.serverType != control.ServerType_Api {
			continue
		}

		config := new[sess.serverID]
		if config.ETag != sess.asConfig.ETag {
			sess.asConfig = config
			sess.trySend(newSessionOut(config))
		}
	}

	return new
}
