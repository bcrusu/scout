package session

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/client"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_                      utils.Lifecycle = (*Session)(nil)
	heartbeatInterval                      = utils.AddJitter(5*time.Second, 0.15)
	sendChSize                             = 32
	getDataServersThrottle                 = utils.AddJitter(2*time.Second, 0.15)
	newSessionThrottle                     = utils.AddJitter(5*time.Second, 0.15)
	log                                    = logging.WithComponent("data_session").NoContext()

	retryBackoff = &utils.Backoff{
		MinDelay: 3 * time.Second,
		MaxDelay: 10 * time.Second,
	}
)

type Session struct {
	client          client.ControlClient
	id              identity.Identity
	address         string
	configPublisher utils.Publisher[*control.DataServerConfig]
	dsPublisher     utils.Publisher[*control.DataServers]
	cancelFunc      context.CancelFunc
}

type stream = grpc.BidiStreamingClient[control.SessionIn, control.SessionOut]

func New(client client.ControlClient, id identity.Identity, address string) *Session {
	return &Session{
		client:          client,
		id:              id,
		address:         address,
		configPublisher: utils.NewPubSub[*control.DataServerConfig](1),
		dsPublisher:     utils.NewPubSub[*control.DataServers](1),
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

func (s *Session) SubscribeDataServerConfig() utils.Subscriber[*control.DataServerConfig] {
	return s.configPublisher.Subscribe(1)
}

func (s *Session) SubscribeDataServers() utils.Subscriber[*control.DataServers] {
	return s.dsPublisher.Subscribe(1)
}

func (m *Session) mainLoop(ctx context.Context) {
	getDataServersThrottled := utils.ThrottleChan(m.dsPublisher.NotifyChan(), getDataServersThrottle)

	for {
		m.runSessionStream(ctx, getDataServersThrottled)

		select {
		case <-ctx.Done():
			return
		case <-time.After(newSessionThrottle):
		}
	}
}

func (m *Session) runSessionStream(ctx context.Context, getDataServersCh <-chan any) {
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

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

	drainLoop := func() {
		for range recvCh {
		}
	}

	for {
		select {
		case err := <-loopDoneCh:
			go drainLoop()
			streamCancel()
			<-loopDoneCh // wait for partner

			if err != nil && !errors.Is(err, io.EOF) {
				log.WithError(err).Warn("Session stream ended abruptly. Reconnecting...")
			} else {
				log.Debug("Session stream ended. Reconnecting...")
			}

			close(sendCh)
			return
		case <-heartbeatTicker.C:
			if gotHello {
				sendCh <- m.newSessionIn(&control.Heartbeat{
					ConfigVersion: config.Version,
				})
			}
		case <-getDataServersCh:
			if gotHello {
				sendCh <- m.newSessionIn(&control.GetDataServers{
					IfNoMatch: dataServers.Version,
				})
			}
		case msg := <-recvCh:
			switch x := msg.(type) {
			case *control.HelloDataServer:
				config = x.Config
				dataServers = x.DataServers
				gotHello = true

				m.dsPublisher.PublishAttempt(dataServers)
				m.configPublisher.PublishAttempt(config)
			case *control.DataServerConfig:
				config = x
				m.configPublisher.PublishAttempt(config)
			case *control.DataServers:
				dataServers = x
				m.dsPublisher.PublishAttempt(dataServers)
			default:
				log.Warnf("Unhandled receive message type %T.", msg)
			}
		// TODO: send DataServerStatus
		case <-ctx.Done():
			go drainLoop()
			streamCancel()
			<-loopDoneCh
			<-loopDoneCh
			return
		}
	}
}

func (m *Session) sendLoop(stream stream, sendCh <-chan *control.SessionIn, doneCh chan<- error) {
	if err := stream.Send(m.newHello()); err != nil {
		doneCh <- errors.Wrapf(err, "failed to send hello")
		return
	}

	for in := range sendCh {
		if err := stream.Send(in); err != nil {
			doneCh <- err
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

	// recvCh <- payload.HelloDataServer
	recvCh <- &control.HelloDataServer{
		Config: &control.DataServerConfig{},
	}

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
		ClusterName: m.id.ClusterName,
		ServerId:    m.id.ServerID,
		Address:     m.address,
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
	return utils.RetryR(ctx, retryBackoff, func() (stream, error) {
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
