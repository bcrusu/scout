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
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_                      utils.Lifecycle = (*Session)(nil)
	heartbeatInterval                      = utils.AddJitter(8*time.Second, 0.10) // TODO: make configurable
	sendChSize                             = 32
	getDataServersThrottle                 = utils.AddJitter(2*time.Second, 0.15)
	log                                    = logging.WithComponent("data_session").NoContext()

	retryBackoff = &utils.Backoff{
		MinDelay: 3 * time.Second,
		MaxDelay: 10 * time.Second,
	}
)

type Session struct {
	client      client.ControlClient
	raft        *multiraft.MultiRaft
	id          identity.Identity
	address     string
	dsPublisher utils.Publisher[*control.DataServers]
	cancelFunc  context.CancelFunc
}

type stream = grpc.BidiStreamingClient[control.SessionIn, control.SessionOut]

func New(client client.ControlClient, raft *multiraft.MultiRaft, id identity.Identity, address string) *Session {
	return &Session{
		client:      client,
		raft:        raft,
		id:          id,
		address:     address,
		dsPublisher: utils.NewPubSub[*control.DataServers](1),
	}
}

func (m *Session) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(m.mainLoop)
	m.cancelFunc = cancelFunc

	go mainLoop(ctx)
	return nil
}

func (m *Session) Stop(ctx context.Context) {
	m.cancelFunc()
}

func (s *Session) SubscribeDataServers() utils.Subscriber[*control.DataServers] {
	return s.dsPublisher.Subscribe(1)
}

func (m *Session) mainLoop(ctx context.Context) {
	getDataServersThrottled := utils.ThrottleChan(m.dsPublisher.NotifyChan(), getDataServersThrottle)

	for {
		streamCtx, streamCancel := context.WithCancel(ctx)
		stream, err := m.newStreamWithRetry(streamCtx)
		if err != nil {
			streamCancel()
			return
		}

		doneCh := make(chan error)
		sendCh := make(chan *control.SessionIn, sendChSize)
		recvCh := make(chan any)

		go m.sendLoop(stream, sendCh, doneCh)
		go m.recvLoop(stream, recvCh, doneCh)

		gotHello := false
		var config *control.DataServerConfig
		var dataServers *control.DataServers

		select {
		case err := <-doneCh:
			go func() {
				for range recvCh {
					// drain
				}
			}()

			streamCancel()
			<-doneCh // wait for partner

			if err != nil && !errors.Is(err, io.EOF) {
				log.WithError(err).Warn("Session stream ended abruptly. Reconnecting...")
			} else {
				log.Debug("Session stream ended. Reconnecting...")
			}

			close(sendCh)
			close(recvCh)
			// TODO: add some random backoff
		case <-getDataServersThrottled:
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
			case *control.DataServerConfig:
				config = x
			case *control.DataServers:
				dataServers = x

				if !m.dsPublisher.PublishAttempt(dataServers) {
					log.Warn("DataServers publish attempt failed.")
				}
			}
		case <-ctx.Done():
			streamCancel()
			<-doneCh
			<-doneCh
			return
		}
	}
}

func (m *Session) sendLoop(stream stream, sendCh <-chan *control.SessionIn, doneCh chan<- error) {
	var lastSend time.Time
	send := func(in *control.SessionIn, what string) bool {
		if err := stream.Send(in); err != nil {
			doneCh <- errors.Wrapf(err, "send %s failed", what)
			return false
		}
		lastSend = time.Now()
		return true
	}

	if !send(m.newHello(), "hello") {
		return
	}

	heartbeat := &control.SessionIn{}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case in := <-sendCh:
			if !send(in, "request") {
				return
			}
		case <-ticker.C:
			if time.Now().After(lastSend.Add(heartbeatInterval)) && !send(heartbeat, "heartbeat") {
				return
			}
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
		doneCh <- errors.Wrap(err, "server send invalid hello.")
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
		ClusterName: m.id.ClusterName,
		ServerId:    m.id.ID,
		Address:     m.address,
	})
}

func (m *Session) newSessionIn(payload any) *control.SessionIn {
	switch p := payload.(type) {
	case *control.Hello:
		return &control.SessionIn{Payload: &control.SessionIn_Hello{Hello: p}}
	case *control.Heartbeat:
		return &control.SessionIn{Payload: &control.SessionIn_Heartbeat{Heartbeat: p}}
	case *control.GetConfig:
		return &control.SessionIn{Payload: &control.SessionIn_GetConfig{GetConfig: p}}
	case *control.GetDataServers:
		return &control.SessionIn{Payload: &control.SessionIn_GetDataServers{GetDataServers: p}}
	case *control.DataServerStatus:
		return &control.SessionIn{Payload: &control.SessionIn_DataServerStatus{DataServerStatus: p}}
	default:
		panic(fmt.Sprintf("unhandled SessionIn payload type %T", payload))
	}
}

func (m *Session) newStreamWithRetry(ctx context.Context) (stream, error) {
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
