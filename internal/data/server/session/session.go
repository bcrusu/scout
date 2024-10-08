package session

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/client"
	"github.com/bcrusu/scout/internal/data/server/events"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_                          utils.Lifecycle = (*Session)(nil)
	heartbeatInterval                          = utils.AddJitter(5*time.Second, 0.15)
	statusInterval                             = utils.AddJitter(30*time.Second, 0.15)
	sendChSize                                 = 32
	refreshDataServersThrottle                 = utils.AddJitter(2*time.Second, 0.15)
	newSessionThrottle                         = utils.AddJitter(5*time.Second, 0.15)
	log                                        = logging.WithComponent("data_session").NoContext()

	retryBackoff = &utils.Backoff{
		MinDelay: 3 * time.Second,
		MaxDelay: 10 * time.Second,
	}
)

type Session struct {
	id         identity.Identity
	address    string
	client     client.ControlClient
	cancelFunc context.CancelFunc
}

type stream = grpc.BidiStreamingClient[control.SessionIn, control.SessionOut]

func New(id identity.Identity, address string, client client.ControlClient) *Session {
	return &Session{
		id:      id,
		address: address,
		client:  client,
	}
}

func (m *Session) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(m.mainLoop)
	m.cancelFunc = cancelFunc

	go mainLoop(ctx)
	return nil
}

func (m *Session) Stop() {
	m.cancelFunc()
}

func (m *Session) mainLoop(ctx context.Context) {
	for {
		m.runSessionStream(ctx)

		select {
		case <-ctx.Done():
			return
		case <-time.After(newSessionThrottle):
		}
	}
}

func (m *Session) runSessionStream(ctx context.Context) {
	replicaStatusSub := eventbus.Subscribe[events.ReplicaStatus]()
	refreshDataServersSub := eventbus.SubscribeThrottled[eventbus.RefreshDataServers](ctx, refreshDataServersThrottle)
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	statusTicker := time.NewTicker(statusInterval)
	defer replicaStatusSub.Unsubscribe()
	defer refreshDataServersSub.Unsubscribe()
	defer heartbeatTicker.Stop()
	defer statusTicker.Stop()

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := m.newSessionWithRetry(streamCtx)
	if err != nil {
		streamCancel()
		return
	}

	loopDoneCh := make(chan error)
	sendCh := make(chan *control.SessionIn, sendChSize)
	recvCh := make(chan any)

	go m.sendLoop(stream, sendCh, loopDoneCh)
	go m.recvLoop(stream, recvCh, loopDoneCh)

	gotHello := false
	var config *control.DataServerConfig
	var dataServers *control.DataServers
	status := &control.DataServerStatus{}

	drainLoop := func() {
		for range recvCh {
		}
	}

	for {
		select {
		case err := <-loopDoneCh:
			go drainLoop()
			streamCancel()

			log.WithError(err).Trace("Session loop done.")
			log.WithError(<-loopDoneCh).Trace("Session loop done.") // wait for partner

			if err != nil && !errors.Is(err, io.EOF) {
				log.WithError(err).Warn("Session stream ended abruptly. Reconnecting...")
			} else {
				log.Debug("Session stream ended. Reconnecting...")
			}

			return
		case <-heartbeatTicker.C:
			if gotHello {
				sendCh <- m.newSessionIn(&control.Heartbeat{
					ConfigETag: config.ETag,
				})
			}
		case <-statusTicker.C:
			if gotHello && len(status.Replicas) != 0 {
				sendCh <- m.newSessionIn(status)
			}
		case <-refreshDataServersSub.Items():
			if gotHello {
				sendCh <- m.newSessionIn(&control.GetDataServers{
					IfNoMatch: dataServers.ETag,
				})
			}
		case x := <-replicaStatusSub.Items():
			status.Replicas = x
		case msg := <-recvCh:
			switch x := msg.(type) {
			case *control.HelloDataServer:
				config = x.Config
				dataServers = x.DataServers
				gotHello = true

				eventbus.TryPublish(dataServers)
				eventbus.TryPublish(config)
			case *control.DataServerConfig:
				config = x
				eventbus.TryPublish(config)
			case *control.DataServers:
				dataServers = x
				eventbus.TryPublish(dataServers)
			default:
				log.Warnf("Unhandled receive message type %T.", msg)
			}
		case <-ctx.Done():
			go drainLoop()
			streamCancel()
			log.WithError(<-loopDoneCh).Trace("Session loop done.")
			log.WithError(<-loopDoneCh).Trace("Session loop done.")
			return
		}
	}
}

func (m *Session) sendLoop(stream stream, sendCh <-chan *control.SessionIn, doneCh chan<- error) {
	if err := stream.Send(m.newHello()); err != nil {
		doneCh <- errors.Wrapf(err, "failed to send hello")
		return
	}

	for {
		select {
		case in := <-sendCh:
			log.Tracef("Sending session message %T.", in.Payload)
			if err := stream.Send(in); err != nil {
				doneCh <- err
				return
			}
		case <-stream.Context().Done():
			doneCh <- nil
			return
		}
	}
}

func (m *Session) recvLoop(stream stream, recvCh chan<- any, doneCh chan<- error) {
	out, err := stream.Recv()
	if err != nil {
		doneCh <- errors.Wrap(err, "new session failed before hello.")
		return
	}

	payload, ok := out.Payload.(*control.SessionOut_HelloDataServer)
	if !ok {
		doneCh <- errors.Error("server did not send hello.")
		return
	} else if err := payload.HelloDataServer.Validate(); err != nil {
		doneCh <- errors.Wrap(err, "server sent invalid hello.")
		return
	}

	recvCh <- payload.HelloDataServer

	for {
		out, err := stream.Recv()
		if err != nil {
			doneCh <- err
			return
		}

		switch x := out.Payload.(type) {
		case *control.SessionOut_HelloDataServer:
			log.Warn("Received duplicate server hello.")
		case *control.SessionOut_DataServerConfig:
			if err := x.DataServerConfig.Validate(); err != nil {
				doneCh <- err
				return
			}
			recvCh <- x.DataServerConfig
		case *control.SessionOut_DataServers:
			if err := x.DataServers.Validate(); err != nil {
				doneCh <- err
				return
			}
			recvCh <- x.DataServers
		default:
			log.Warnf("Unknown session payload type %T", out.Payload)
		}
	}
}

func (m *Session) newHello() *control.SessionIn {
	return m.newSessionIn(&control.Hello{
		ServerId: m.id.ServerID,
		Address:  m.address,
	})
}

func (m *Session) newSessionIn(payload any) *control.SessionIn {
	switch p := payload.(type) {
	case *control.Hello:
		return &control.SessionIn{Payload: &control.SessionIn_Hello{Hello: p}}
	case *control.Heartbeat:
		return &control.SessionIn{Payload: &control.SessionIn_Heartbeat{Heartbeat: p}}
	case *control.GetDataServers:
		return &control.SessionIn{Payload: &control.SessionIn_GetDataServers{GetDataServers: p}}
	case *control.DataServerStatus:
		return &control.SessionIn{Payload: &control.SessionIn_DataServerStatus{DataServerStatus: p}}
	default:
		panic(fmt.Sprintf("unhandled SessionIn payload type %T", payload))
	}
}

func (m *Session) newSessionWithRetry(ctx context.Context) (stream, error) {
	return utils.RetryForeverR(ctx, retryBackoff, func() (stream, error) {
		stream, err := m.client.NewSession(ctx)
		if err != nil {
			log.WithError(err).Error("NewSession call failed. Retrying...")
			return nil, err
		} else {
			log.Debug("NewSession call success.")
		}

		return stream, err
	})
}
