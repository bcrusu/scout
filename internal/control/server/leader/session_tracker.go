package leader

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/convert"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"github.com/bcrusu/graph/internal/utils"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

const (
	sessionSendBufferChSize = 32
	updateServersInterval   = 5 * time.Second
)

var (
	logS                            = logging.WithComponent("session_tracker")
	_               utils.Lifecycle = (*sessionTracker)(nil)
	recvBurst                       = 5
	recvLimit                       = rate.Limit(float64(recvBurst) / float64(time.Second))
	recvMaxOffenses                 = 16 // after this the session will be closed
)

type sessionTracker struct {
	store                 storage.Store
	commandCh             chan any
	sessionCh             chan sessionMessage
	cancelFunc            context.CancelFunc
	dataServiceConfigJson string
	apiServiceConfigJson  string
	dataServers           atomic.Value // *DataServers
	dataServersVersion    atomic.Uint64
	apiServers            atomic.Value // *ApiServers
	apiServersVersion     atomic.Uint64
}

type session struct {
	id            sessionID
	serverID      serverID
	serverType    control.ServerType
	serverAddress string
	createdAt     time.Time
	receivedAt    time.Time
	sendBufferCh  chan *control.SessionOut
	ctx           context.Context
	log           logging.Logger
	waitCh        chan error
	recvLimiter   *rate.Limiter // dedicated for recv loop
	recvOffenses  int           // dedicated for recv loop
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

func newSessionTracker(store storage.Store) *sessionTracker {
	dscfg := serviceconfig.DefaultServiceConfig().WithLBGraphData()
	ascfg := serviceconfig.DefaultServiceConfig().WithLBGraphApi()

	return &sessionTracker{
		store:                 store,
		commandCh:             make(chan any),
		sessionCh:             make(chan sessionMessage, 1),
		dataServiceConfigJson: dscfg.ToJson(),
		apiServiceConfigJson:  ascfg.ToJson(),
	}
}

func (t *sessionTracker) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(t.mainLoop)
	t.cancelFunc = cancelFunc

	go mainLoop(ctx)
	return nil
}

func (t *sessionTracker) Stop(ctx context.Context) {
	t.cancelFunc()
}

func (t *sessionTracker) NewSession(stream sessionStream) error {
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

	t.commandCh <- cmd

	// Wait until we get the signal from the main loop below. Once
	// this method returns, gRPC will close the stream resulting in both
	// send and receive loops to end.
	return <-cmd.waitCh
}

func (t *sessionTracker) mainLoop(ctx context.Context) {
	updateServersTicker := time.NewTicker(updateServersInterval)
	defer updateServersTicker.Stop()

	sessionCounter := sessionID(0)
	sessionsById := sessions{}
	sessionsByServer := sessionsByServer{}
	partitions := t.initPartitionState()

	t.updateDataServers(sessionsByServer, partitions)
	t.updateApiServers(sessionsByServer)

	// method will be called at least twice:
	//  - once for each ending send/recv loop
	//  - during stop
	//  - when a new session replaces an old one
	//  - and any other scenarios when the server decides to end the stream.
	closeSession := func(id sessionID, err error) {
		if sess, ok := sessionsById[id]; ok {
			delete(sessionsById, sess.id)
			delete(sessionsByServer, sess.serverID)
			sess.log.Debug(sess.ctx, "Session closed.")
			sess.waitCh <- err // signals the waiting NewSession call above
		}
	}

	drainLoop := func() {
		for {
			select {
			case cmd := <-t.commandCh:
				logS.Warnf(ctx, "Received command %T after Stop.", cmd)
			case <-t.sessionCh:
				// drop
			}
		}
	}

	for {
		select {
		//TODO: watch FSM changes
		case <-ctx.Done():
			go drainLoop()

			for id := range sessionsById {
				closeSession(id, nil)
			}
			return
		case cmd := <-t.commandCh:
			switch x := cmd.(type) {
			case startSession:
				server := t.store.Server(uint64(x.serverID))
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
					receivedAt:    now,
					sendBufferCh:  make(chan *control.SessionOut, sessionSendBufferChSize),
					ctx:           x.stream.Context(),
					waitCh:        x.waitCh,
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

				go t.startSessionLoops(new, x.stream)

				if !needsUpdate {
					continue
				}

				switch new.serverType {
				case control.ServerType_Data:
					t.updateDataServers(sessionsByServer, partitions)
				case control.ServerType_Api:
					t.updateApiServers(sessionsByServer)
				}
			default:
				logS.Errorf(ctx, "Unknown command type %T", cmd)
			}
		case msg := <-t.sessionCh:
			sess := sessionsById[msg.ID()]
			if sess == nil {
				continue
			}

			switch x := msg.(type) {
			case endSession:
				closeSession(x.id, x.err)
			case sessionReceived:
				sessionsById[x.id].receivedAt = time.Now().UTC()
			case updateLeader:
				for id, term := range x.currentTerm {
					part := partitions[id]
					part.hasLeader = true
					if term > part.leaderTerm {
						part.leaderID = sess.serverID
						part.leaderTerm = term
					}
				}
				// TODO: throttle
				t.updateDataServers(sessionsByServer, partitions)
			default:
				logS.Errorf(ctx, "Unknown message type %T", msg)
			}
		case <-updateServersTicker.C:
			// TODO
			//t.store.ApplyUpdateServersAsync()
		}
	}
}

func (t *sessionTracker) initPartitionState() partitionStates {
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

func (t *sessionTracker) startSessionLoops(sess *session, stream sessionStream) {
	go t.sessionSendLoop(sess, stream)
	go t.sessionRecvLoop(sess, stream)

	// enqueue server hello
	var out *control.SessionOut

	switch sess.serverType {
	case control.ServerType_Data:
		out = t.newSessionOut(&control.HelloDataServer{
			Config:      nil, // TODO
			DataServers: t.dataServers.Load().(*control.DataServers),
		})
	case control.ServerType_Api:
		out = t.newSessionOut(&control.HelloApiServer{
			Config:      nil, // TODO
			DataServers: t.dataServers.Load().(*control.DataServers),
			ApiServers:  t.apiServers.Load().(*control.ApiServers),
		})
	}

	if out != nil {
		sess.sendBufferCh <- out
	}
}

func (t *sessionTracker) sessionSendLoop(sess *session, stream sessionStream) {
	for out := range sess.sendBufferCh {
		err := stream.Send(out)
		if err == nil {
			continue
		}

		if err != io.EOF {
			sess.log.WithError(err).Error(sess.ctx, "Session send failed.")
		} else {
			sess.log.Debug(sess.ctx, "Session send loop done.")
		}

		t.sessionCh <- endSession{id: sess.id, err: nil}
		return
	}
}

func (t *sessionTracker) sessionRecvLoop(sess *session, stream sessionStream) {
	endSession := func(err error) {
		t.sessionCh <- endSession{id: sess.id, err: err}
	}

	for {
		in, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				sess.log.WithError(err).Error(sess.ctx, "Session receive failed.")
			} else {
				sess.log.Debug(sess.ctx, "Session receive loop done.")
			}
			endSession(nil)
			return
		}

		if !sess.recvLimiter.Allow() {
			sess.recvOffenses++
			if sess.recvOffenses == recvMaxOffenses {
				endSession(errors.ResourceExhausted)
				return
			}
			sess.log.Error(sess.ctx, "Session triggered receive rate limiter. Dropping message.")
			continue
		}

		sess.recvOffenses = 0
		t.sessionCh <- sessionReceived{id: sess.id}

		switch x := in.Payload.(type) {
		case *control.SessionIn_Hello:
			sess.log.Warn(sess.ctx, "Received duplicate hello.")
		case *control.SessionIn_Heartbeat:
			// nop
		case *control.SessionIn_GetConfig:
			if err := t.handleGetConfig(sess, x.GetConfig); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_GetDataServers:
			if err := t.handleGetDataServers(sess, x.GetDataServers); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_GetApiServers:
			if err := t.handleGetApiServers(sess, x.GetApiServers); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_DataServerStatus:
			if err := t.handleDataServerStatus(sess, x.DataServerStatus); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_ApiServerStatus:
			if err := t.handleApiServerStatus(sess, x.ApiServerStatus); err != nil {
				endSession(err)
				return
			}
		default:
			sess.log.Warnf(sess.ctx, "Unknown session payload type %T", in.Payload)
		}
	}
}

func (t *sessionTracker) handleGetDataServers(sess *session, msg *control.GetDataServers) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid GetDataServers request.")
		return errors.InvalidRequest
	} else if msg.IfNoMatch != 0 && msg.IfNoMatch == t.dataServersVersion.Load() {
		return nil
	}

	ds := t.dataServers.Load().(*control.DataServers)
	sess.trySend(t.newSessionOut(ds))
	return nil
}

func (t *sessionTracker) handleGetApiServers(sess *session, msg *control.GetApiServers) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid GetApiServers request.")
		return errors.InvalidRequest
	} else if msg.IfNoMatch != 0 && msg.IfNoMatch == t.apiServersVersion.Load() {
		return nil
	}

	as := t.apiServers.Load().(*control.ApiServers)
	sess.trySend(t.newSessionOut(as))
	return nil
}

func (t *sessionTracker) handleGetConfig(sess *session, msg *control.GetConfig) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid GetConfig request.")
		return errors.InvalidRequest
	}

	// if msg.IfNoMatch != 0 && msg.IfNoMatch == sess.lastConfigVersion {
	// 	return nil
	// }
	// TODO:
	return nil
}

func (t *sessionTracker) handleDataServerStatus(sess *session, msg *control.DataServerStatus) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid DataServerStatus request.")
		return errors.InvalidRequest
	} else if sess.serverType != control.ServerType_Data {
		return errors.PermissionDenied
	}

	count := t.store.PartitionCount()

	cmd := updateLeader{
		id:          sess.id,
		currentTerm: map[partitionID]uint64{},
	}

	for id, part := range msg.Partitions {
		if id >= count {
			return errors.InvalidRequest
		}

		if part.Leader {
			cmd.currentTerm[partitionID(id)] = part.LeaderTerm
		}
	}

	if len(cmd.currentTerm) > 0 {
		t.sessionCh <- cmd
	}

	return nil
}

func (t *sessionTracker) handleApiServerStatus(sess *session, msg *control.ApiServerStatus) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid ApiServerStatus request.")
		return errors.InvalidRequest
	} else if sess.serverType != control.ServerType_Api {
		return errors.PermissionDenied
	}

	return nil
}

func (t *sessionTracker) updateDataServers(sessions sessionsByServer, partitionStates partitionStates) {
	servers := t.store.Servers()
	partitions := t.store.Partitions()

	ds := &control.DataServers{
		Version:           t.dataServersVersion.Add(1),
		Servers:           map[uint64]*control.DataServers_Server{},
		Partitions:        map[uint32]*control.DataServers_Partition{},
		PartitionCount:    t.store.PartitionCount(),
		ServiceConfigJson: t.dataServiceConfigJson,
	}

	for id, server := range servers.ForType(storage.ServerType_Data) {
		ds.Servers[id] = &control.DataServers_Server{
			Id:      id,
			Address: server.LastAddress,
		}

		if sess := sessions[serverID(id)]; sess != nil {
			ds.Servers[id].Address = sess.serverAddress
		}
	}

	for id, part := range partitions.Items {
		readServers := make([]uint64, len(part.GroupMembers))
		for i, member := range part.GroupMembers {
			readServers[i] = member.ServerId
		}

		ds.Partitions[id] = &control.DataServers_Partition{
			Id:          id,
			ReadServers: readServers,
		}

		if state := partitionStates[partitionID(id)]; state.hasLeader {
			ds.Partitions[id].WriteServer = uint64(state.leaderID)
		}
	}

	t.dataServers.Store(ds)
	sessions.trySendAll(t.newSessionOut(ds))
}

func (t *sessionTracker) updateApiServers(sessions sessionsByServer) {
	servers := t.store.Servers()

	as := &control.ApiServers{
		Version:           t.apiServersVersion.Add(1),
		Servers:           map[uint64]*control.ApiServers_Server{},
		ServiceConfigJson: t.apiServiceConfigJson,
	}

	for id, server := range servers.ForType(storage.ServerType_Api) {
		as.Servers[id] = &control.ApiServers_Server{
			Id:      id,
			Address: server.LastAddress,
		}

		if sess := sessions[serverID(id)]; sess != nil {
			as.Servers[id].Address = sess.serverAddress
		}
	}

	t.apiServers.Store(as)

	out := t.newSessionOut(as)
	for _, sess := range sessions {
		if sess.serverType == control.ServerType_Api {
			sess.trySend(out)
		}
	}
}

func (t *sessionTracker) newSessionOut(payload any) *control.SessionOut {
	switch p := payload.(type) {
	case *control.HelloDataServer:
		return &control.SessionOut{Payload: &control.SessionOut_HelloDataServer{HelloDataServer: p}}
	case *control.HelloApiServer:
		return &control.SessionOut{Payload: &control.SessionOut_HelloApiServer{HelloApiServer: p}}
	case *control.DataServerConfig:
		return &control.SessionOut{Payload: &control.SessionOut_DataServerConfig{DataServerConfig: p}}
	case *control.ApiServerConfig:
		return &control.SessionOut{Payload: &control.SessionOut_ApiServerConfig{ApiServerConfig: p}}
	case *control.DataServers:
		return &control.SessionOut{Payload: &control.SessionOut_DataServers{DataServers: p}}
	case *control.ApiServers:
		return &control.SessionOut{Payload: &control.SessionOut_ApiServers{ApiServers: p}}
	default:
		panic(fmt.Sprintf("unhandled SessionOut payload type %T", payload))
	}
}

func (s *session) trySend(out *control.SessionOut) {
	select {
	case s.sendBufferCh <- out:
	default:
		s.log.Warnf(s.ctx, "Session send buffer is full. Message %T was dropped.", out)
	}
}

func (s sessionsByServer) trySendAll(out *control.SessionOut) {
	for _, sess := range s {
		sess.trySend(out)
	}
}
