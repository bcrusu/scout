package sessions

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/api"
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/pkg/graph"
	"github.com/bcrusu/scout/pkg/keyvalue"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

var (
	logS                             = logging.New("session_tracker")
	_                utils.Lifecycle = (*Tracker)(nil)
	debounceInterval                 = 20 * time.Millisecond
)

type Tracker struct {
	config                config.Config
	store                 storage.Store
	startSessionCh        chan startSession
	sessionCh             chan sessionMessage
	cancelFunc            context.CancelFunc
	dataServiceConfigJson string
	apiServiceConfigJson  string
	dataServers           atomic.Pointer[control.DataServers]
	apiServers            atomic.Pointer[control.ApiServers]
	meters                *trackerMeters
}

type session struct {
	tracker       *Tracker
	id            sessionID
	serverID      uint64
	serverType    control.ServerType
	serverName    string
	serverAddress string
	createdAt     time.Time
	sendBufferCh  chan *control.SessionOut
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

func NewTracker(store storage.Store) *Tracker {
	c := config.Get()

	return &Tracker{
		config:                c,
		store:                 store,
		startSessionCh:        make(chan startSession),
		sessionCh:             make(chan sessionMessage, 1),
		dataServiceConfigJson: c.Service.Data.GetServiceConfigJson(serviceconfig.LBNameScoutData, data.Service_ServiceDesc),
		apiServiceConfigJson:  c.Service.Api.GetServiceConfigJson(serviceconfig.LBNameScoutApi, api.Service_ServiceDesc, keyvalue.Service_ServiceDesc, graph.Service_ServiceDesc),
		meters:                newTrackerMeters(),
	}
}

func (t *Tracker) Start(ctx context.Context) error {
	t.cancelFunc = utils.RunAsync(ctx, t.mainLoop)
	return nil
}

func (t *Tracker) Stop() {
	t.cancelFunc()
	t.meters.Unregister()
}

func (t *Tracker) NewSession(stream sessionStream) error {
	in, err := stream.Recv()
	if err != nil {
		return err
	}

	payload, ok := in.Payload.(*control.SessionIn_Hello)
	if !ok {
		return errors.ValidationError{Message: "Please don't be rude."}
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

	t.meters.SessionCount.Add(1)
	err = <-cmd.waitCh
	t.meters.SessionCount.Add(-1)
	return err
}

func (t *Tracker) mainLoop(ctx context.Context) {
	serversSub := eventbus.Subscribe[*control.Servers]()
	partitionsSub := eventbus.Subscribe[*control.Partitions]()
	writeLatestStatusTicker := time.NewTicker(t.config.Sessions.WriteStatusInterval)
	defer serversSub.Unsubscribe()
	defer partitionsSub.Unsubscribe()
	defer writeLatestStatusTicker.Stop()

	dsUpdateCh, dsUpdateChDb := utils.MakeDebounceChan[bool](ctx, debounceInterval, 1)
	asUpdateCh, asUpdateChDb := utils.MakeDebounceChan[bool](ctx, debounceInterval, 1)
	dsConfigsUpdateCh, dsConfigsUpdateChDb := utils.MakeDebounceChan[bool](ctx, debounceInterval, 1)
	asConfigsUpdateCh, asConfigsUpdateChDb := utils.MakeDebounceChan[bool](ctx, debounceInterval, 1)

	servers := t.store.Servers()
	partitions := t.store.Partitions()
	status := newStatusTracker(servers, partitions)
	dsConfigs := t.makeDataServerConfigs(servers, partitions)
	asConfigs := t.makeApiServerConfigs(servers)

	sessionCounter := sessionID(0)
	sessionsById := sessions{}
	sessionsByServer := sessionsByServer{}
	runningLoops := 0

	t.updateDataServerList(sessionsById, servers, partitions, status)
	t.updateApiServerList(sessionsById, servers, status)

	closeSession := func(id sessionID, err error) {
		if sess, ok := sessionsById[id]; ok {
			delete(sessionsById, sess.id)
			delete(sessionsByServer, sess.serverID)
			sess.log.Debug("Session closed.")
			sess.waitCh <- err // signals the waiting NewSession call above
		}
	}

	for {
		select {
		case x := <-t.startSessionCh:
			server := servers.Items[x.serverID]
			if server == nil {
				logS.WithContext(ctx).Warnf("Session hello for unknown server %d at %s.", x.serverID, x.serverAddress)
				x.waitCh <- errors.NotRegistered
				continue
			}
			if server.Type == control.ServerType_Control {
				logS.WithContext(ctx).Warnf("Control server %d at %s trying to start session.", x.serverID, x.serverAddress)
				x.waitCh <- errors.PermissionDenied
				continue
			}

			now := time.Now().UTC()
			sessionCounter++
			needsUpdate := true

			new := &session{
				tracker:       t,
				id:            sessionCounter,
				serverID:      x.serverID,
				serverType:    server.Type,
				serverName:    server.Name,
				serverAddress: x.serverAddress,
				createdAt:     now,
				sendBufferCh:  make(chan *control.SessionOut, t.config.Sessions.SendBufferSize),
				waitCh:        x.waitCh,
				dsConfig:      dsConfigs[x.serverID],
				asConfig:      asConfigs[x.serverID],
				recvLimiter:   utils.NewRateLimiter(t.config.Sessions.ReceiveBurst, time.Second),
			}

			new.log = logging.New("session").With("server", server.Id, "session_id", new.id, "address", new.serverAddress).WithContext(x.stream.Context())

			if old := sessionsByServer[x.serverID]; old != nil {
				new.log.Debugf("Closing old session %d created at %v.", old.id, old.createdAt)
				needsUpdate = old.serverAddress != new.serverAddress
				closeSession(old.id, nil)
			}

			sessionsById[new.id] = new
			sessionsByServer[new.serverID] = new
			status.recordNewSession(new)

			go t.sessionSendLoop(new, x.stream)
			go t.sessionRecvLoop(new, x.stream)
			runningLoops += 2

			new.log.Info("Started new session.")

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
				logS.WithContext(ctx).Errorf("Unknown session message type %T", msg)
			}
		case newServers := <-serversSub.Items():
			if newServers.RegisterVersion == servers.RegisterVersion {
				continue
			}

			// close sessions for removed servers
			for serverID, sess := range sessionsByServer {
				if newServers.Items[serverID] == nil {
					closeSession(sess.id, errors.NotRegistered)
				}
			}

			servers = newServers
			status.syncServers(newServers)
			dsUpdateCh <- true
			asUpdateCh <- true
			dsConfigsUpdateCh <- true
			asConfigsUpdateCh <- true
		case newPartitions := <-partitionsSub.Items():
			if newPartitions.AssignmentsVersion == partitions.AssignmentsVersion {
				continue
			}

			partitions = newPartitions
			status.syncPartitions(newPartitions)
			dsUpdateCh <- true
			dsConfigsUpdateCh <- true
		case <-writeLatestStatusTicker.C:
			t.writeLatestStatus(ctx, status)
		case <-dsUpdateChDb:
			t.updateDataServerList(sessionsById, servers, partitions, status)
		case <-asUpdateChDb:
			t.updateApiServerList(sessionsById, servers, status)
		case <-dsConfigsUpdateChDb:
			dsConfigs = t.updateDataServerConfigs(sessionsById, servers, partitions)
		case <-asConfigsUpdateChDb:
			asConfigs = t.updateApiServerConfigs(sessionsById, servers)
		case <-ctx.Done():
			goto SHUTDOWN
		}
	}

SHUTDOWN:
	for id := range sessionsById {
		closeSession(id, nil)
	}

	t.writeLatestStatus(ctx, status)
	status.Unregister()

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

func (t *Tracker) updateDataServerList(sessions sessions, servers *control.Servers, partitions *control.Partitions, status *statusTracker) {
	new := &control.DataServers{
		Servers:           map[uint64]*control.DataServers_Server{},
		Partitions:        map[uint32]*control.DataServers_Partition{},
		PartitionCount:    t.store.PartitionCount(),
		ServiceConfigJson: t.dataServiceConfigJson,
	}

	for id := range servers.DataServers() {
		new.Servers[id] = &control.DataServers_Server{
			Id:      id,
			Address: status.getServerAddress(id),
		}
	}

	for id, part := range partitions.Items {
		leaderServerId := uint64(0)
		replicaServerIDs := make([]uint64, 0, len(part.Replicas))

		for _, replica := range part.Replicas {
			if replica.State.IsServing() {
				replicaServerIDs = append(replicaServerIDs, replica.ServerId)
			}
		}
		slices.Sort(replicaServerIDs)

		if leader := status.getPartitionLeader(id); leader != "" {
			replica := part.Replicas[leader]
			if replica != nil {
				leaderServerId = replica.ServerId
			}
		}

		new.Partitions[id] = &control.DataServers_Partition{
			ETag:             makeETag(fmt.Sprintf("%d:%v", leaderServerId, replicaServerIDs)),
			Id:               id,
			LeaderServerId:   leaderServerId,
			ReplicaServerIds: replicaServerIDs,
		}
	}

	etags := make([]string, 0, len(new.Partitions)+len(new.Servers))
	for id, server := range new.Servers {
		etags = append(etags, fmt.Sprintf("srv %d:%s", id, server.Address))
	}
	for id, part := range new.Partitions {
		etags = append(etags, fmt.Sprintf("part %d:%s", id, part.ETag))
	}

	new.ETag = makeETag(etags...)
	if old := t.dataServers.Load(); old != nil && new.ETag == old.ETag {
		return
	}

	t.dataServers.Store(new)

	out := newSessionOut(new)
	sessions.trySendAll(out)
}

func (t *Tracker) updateApiServerList(sessions sessions, servers *control.Servers, status *statusTracker) {
	new := &control.ApiServers{
		Servers:           map[uint64]*control.ApiServers_Server{},
		ServiceConfigJson: t.apiServiceConfigJson,
	}

	for id := range servers.ApiServers() {
		new.Servers[id] = &control.ApiServers_Server{
			Id:      id,
			Address: status.getServerAddress(id),
		}
	}

	etags := make([]string, 0, len(new.Servers))
	for id, server := range new.Servers {
		etags = append(etags, fmt.Sprintf("srv %d:%s", id, server.Address))
	}

	new.ETag = makeETag(etags...)
	if old := t.dataServers.Load(); old != nil && new.ETag == old.ETag {
		return
	}

	t.apiServers.Store(new)

	out := newSessionOut(t.apiServers.Load())
	sessions.trySendServerType(out, control.ServerType_Api)
}

func (t *Tracker) updateDataServerConfigs(sessions sessions, servers *control.Servers, partitions *control.Partitions) dsConfigs {
	new := t.makeDataServerConfigs(servers, partitions)

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

func (t *Tracker) updateApiServerConfigs(sessions sessions, servers *control.Servers) asConfigs {
	new := t.makeApiServerConfigs(servers)

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
