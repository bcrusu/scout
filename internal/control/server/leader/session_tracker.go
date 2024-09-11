package leader

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/server/convert"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

const (
	sessionSendBufferChSize = 16
	updateServersInterval   = 5 * time.Second
)

var (
	logS                 = logging.WithComponent("session_tracker")
	_    utils.Lifecycle = (*sessionTracker)(nil)
)

type sessionTracker struct {
	store      storage.Store
	commandCh  chan any
	sessionCh  chan any
	cancelFunc context.CancelFunc
}

type session struct {
	id           uint64
	ctx          context.Context
	stream       sessionStream
	serverID     uint64
	serverType   control.ServerType
	wg           sync.WaitGroup
	createdAt    time.Time
	sendBufferCh chan *control.SessionOut
	log          logging.Logger
}

type sessionStream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]

func newSessionTracker(store storage.Store) *sessionTracker {
	return &sessionTracker{
		store:     store,
		commandCh: make(chan any),
		sessionCh: make(chan any),
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
	hello, err := stream.Recv()
	if err != nil {
		return errors.Wrap(err, "new session failed before hello")
	}

	if hello == nil || hello.ClusterName != t.store.ClusterName() || hello.ServerId == 0 || hello.Address == "" {
		return errors.InvalidRequest
	}

	cmd := startSessionCmd{
		stream: stream,
		hello:  hello,
		result: make(chan startSessionRes),
	}

	t.commandCh <- cmd

	result := <-cmd.result
	if result.err != nil {
		return result.err
	}

	// Wait until we get the signal from the session tracker below. Once
	// this method returns, gRPC will close the stream resulting in both
	// send and receive loops to end.
	result.wg.Wait()
	return nil
}

func (t *sessionTracker) mainLoop(ctx context.Context) {
	updateServersTicker := time.NewTicker(updateServersInterval)
	defer updateServersTicker.Stop()

	sessionCounter := uint64(0)
	sessionsById := map[uint64]*session{}
	sessionsByServer := map[uint64]*session{}
	lastSeen := map[uint64]time.Time{} // server last seen time
	lastAddress := map[uint64]string{} // server last address

	// method will be called at least twice:
	//  - once for each ending send/recv loop
	//  - when a new session replaces an old one
	//  - and any other scenarios when the server decides to end the stream.
	closeSession := func(sess *session) {
		if _, ok := sessionsById[sess.id]; !ok {
			return
		}
		sess.wg.Done() // signals the waiting NewSession call above
		delete(sessionsById, sess.id)
		delete(sessionsByServer, sess.serverID)
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
		case <-ctx.Done():
			go drainLoop()

			for _, sess := range sessionsById {
				sess.wg.Done()
			}
			return
		case cmd := <-t.commandCh:
			switch x := cmd.(type) {
			case startSessionCmd:
				server := t.store.Server(x.hello.ServerId)
				if server == nil {
					logS.Warnf(ctx, "Session hello for unknwon server %d at %s.", x.hello.ServerId, x.hello.Address)
					x.result <- startSessionRes{err: errors.NotRegistered}
					continue
				}

				serverType := x.hello.ServerType()
				if server.Type != convert.ToServerType(serverType) {
					logS.Errorf(ctx, "Session hello with bad server type %v at %s. Expected %v.", serverType, x.hello.Address, server.Type)
					x.result <- startSessionRes{err: errors.InvalidRequest}
					continue
				}

				sessionCounter++
				id := sessionCounter

				log := server.AddToLog(logS).With("session_id", id, "address", x.hello.Address)

				if old := sessionsByServer[server.Id]; old != nil {
					log.Debugf(old.stream.Context(), "Closing old session %d created at %v.", old.id, old.createdAt)
					closeSession(old)
				}

				new := &session{
					id:           id,
					ctx:          x.stream.Context(),
					stream:       x.stream,
					serverID:     server.Id,
					serverType:   serverType,
					createdAt:    time.Now().UTC(),
					sendBufferCh: make(chan *control.SessionOut, sessionSendBufferChSize),
					log:          log,
				}

				sessionsById[new.id] = new
				sessionsByServer[server.Id] = new
				lastSeen[server.Id] = new.createdAt
				lastAddress[server.Id] = x.hello.Address

				t.startSessionLoops(new)

				new.wg.Add(1)
				x.result <- startSessionRes{wg: &new.wg}
			default:
				logS.Errorf(ctx, "Unknown command type %T", cmd)
			}
		case msg := <-t.sessionCh:
			switch x := msg.(type) {
			case sessionLoopEnd:
				closeSession(x.session)
			case sessionHeartbeat:
				lastSeen[x.session.serverID] = time.Now().UTC()
			default:
				logS.Errorf(ctx, "Unknown message type %T", msg)
			}
		case <-updateServersTicker.C:
			// TODO
			//t.store.ApplyUpdateServersAsync()
		}
	}
}

func (t *sessionTracker) startSessionLoops(session *session) {
	go t.sessionSendLoop(session)
	go t.sessionRecvLoop(session)
}

func (t *sessionTracker) sessionSendLoop(sess *session) {
	for out := range sess.sendBufferCh {
		err := sess.stream.Send(out)
		if err == nil {
			continue
		}

		if err != io.EOF {
			sess.log.WithError(err).Error(sess.ctx, "Session send failed.")
		} else {
			sess.log.Debug(sess.ctx, "Session send loop done.")
		}

		t.sessionCh <- sessionLoopEnd{session: sess}
		return
	}
}

func (t *sessionTracker) sessionRecvLoop(sess *session) {
	for {
		in, err := sess.stream.Recv()

		switch {
		case err != nil:
			if err != io.EOF {
				sess.log.WithError(err).Error(sess.ctx, "Session receive failed.")
			} else {
				sess.log.Debug(sess.ctx, "Session receive loop done.")
			}

			t.sessionCh <- sessionLoopEnd{session: sess}
			return
		case in.ServerType() != sess.serverType:
			sess.log.Errorf(sess.ctx, "Session request with bad type %v. Closing...", in.ServerType())
			t.sessionCh <- sessionLoopEnd{session: sess}
			return
		}

		t.sessionCh <- sessionHeartbeat{session: sess}
		if in.IsHeartbeat() {
			continue
		}

		switch x := in.Payload.(type) {
		case *control.SessionIn_Control:
			t.handleControlReq(sess, x.Control)
		case *control.SessionIn_Data:
			t.handleDataReq(sess, x.Data)
		case *control.SessionIn_Api:
			t.handleApiReq(sess, x.Api)
		}
	}
}

func (t *sessionTracker) handleControlReq(sess *session, req *control.SessionIn_ControlReq) {

}

func (t *sessionTracker) handleDataReq(sess *session, req *control.SessionIn_DataReq) {

}

func (t *sessionTracker) handleApiReq(sess *session, req *control.SessionIn_ApiReq) {

}
