package session

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bcrusu/scout/internal/api/server/config"
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/client"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	_                          utils.Lifecycle = (*Session)(nil)
	refreshDataServersThrottle                 = utils.AddJitter(2 * time.Second)
	log                                        = logging.New("api_session").NoContext()
)

type Session struct {
	config     config.Session
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
	m.cancelFunc = utils.RunAsync(ctx, m.mainLoop)
	return nil
}

func (m *Session) Stop() {
	m.cancelFunc()
}

func (m *Session) mainLoop(ctx context.Context) {
	for {
		err := m.runSessionStream(ctx)

		switch {
		case err != nil && !errors.Is(err, io.EOF):
			log.WithError(err).Warn("Session stream ended abruptly. Reconnecting...")
		case errors.Is(err, context.Canceled):
			return
		case errors.Is(err, errors.TimeOffsetOutOfRange):
			utils.GracefulShutdown("Time offset is out of allowed range.")
			return
		default:
			log.Debug("Session stream ended. Reconnecting...")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(utils.AddJitter(m.config.NewSessionThrottle)):
		}
	}
}

func (m *Session) runSessionStream(ctx context.Context) error {
	refreshDataServersSub := eventbus.SubscribeThrottled[eventbus.RefreshDataServers](ctx, refreshDataServersThrottle)
	heartbeatTicker := time.NewTicker(utils.AddJitter(m.config.HeartbeatInterval))
	statusTicker := time.NewTicker(utils.AddJitter(m.config.StatusInterval))
	defer refreshDataServersSub.Unsubscribe()
	defer heartbeatTicker.Stop()
	defer statusTicker.Stop()

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := m.client.NewSession(streamCtx)
	if err != nil {
		streamCancel()
		return err
	}

	loopDoneCh := make(chan error)
	sendCh := make(chan *control.SessionIn, m.config.SendBufferSize)
	recvCh := make(chan any)

	go m.sendLoop(stream, sendCh, loopDoneCh)
	go m.recvLoop(stream, recvCh, loopDoneCh)

	gotHello := false
	var config *control.ApiServerConfig
	var apiServers *control.ApiServers
	var dataServers *control.DataServers
	status := &control.ApiServerStatus{}

	drainLoop := func() {
		for range recvCh {
		}
	}

	closeSession := func(firstErr error) {
		go drainLoop()
		streamCancel()

		if firstErr != nil {
			log.WithError(firstErr).Trace("Session loop done.")
		} else {
			log.WithError(<-loopDoneCh).Trace("Session loop done.")
		}
		log.WithError(<-loopDoneCh).Trace("Session loop done.") // wait for partner
		close(recvCh)
	}

	for {
		select {
		case err := <-loopDoneCh:
			closeSession(err)
			return err
		case <-heartbeatTicker.C:
			if gotHello {
				sendCh <- m.newSessionIn(&control.Heartbeat{
					ConfigETag: config.ETag,
				})
			}
		case <-statusTicker.C:
			if gotHello {
				sendCh <- m.newSessionIn(status)
			}
		case <-refreshDataServersSub.Items():
			if gotHello {
				sendCh <- m.newSessionIn(&control.GetDataServers{
					IfNoMatch: dataServers.ETag,
				})
			}
		case msg := <-recvCh:
			switch x := msg.(type) {
			case *control.HelloApiServer:
				if err := hlc.Update(x.Timestamp); err != nil {
					log.WithError(err).Error("Failed to update HLC with hello timestamp.")
					closeSession(nil)
					return err
				}

				config = x.Config
				apiServers = x.ApiServers
				dataServers = x.DataServers
				gotHello = true

				eventbus.TryPublish(apiServers)
				eventbus.TryPublish(dataServers)
				eventbus.TryPublish(config)
			case *control.ApiServerConfig:
				if x.ETag != config.ETag {
					config = x
					eventbus.TryPublish(config)
				}
			case *control.DataServers:
				if x.ETag != dataServers.ETag {
					dataServers = x
					eventbus.TryPublish(dataServers)
				}
			case *control.ApiServers:
				apiServers = x
				eventbus.TryPublish(apiServers)
			case *control.TimestampRequest:
				sendCh <- m.newSessionIn(&control.TimestampResponse{
					RequestTimestamp:  x.RequestTimestamp,
					ResponseTimestamp: timestamppb.Now(),
				})
			default:
				log.Warnf("Unhandled receive message type %T.", msg)
			}
		case <-ctx.Done():
			closeSession(nil)
			return ctx.Err()
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

	payload, ok := out.Payload.(*control.SessionOut_HelloApiServer)
	if !ok {
		doneCh <- errors.Error("server did not send hello.")
		return
	}

	recvCh <- payload.HelloApiServer

	for {
		out, err := stream.Recv()
		if err != nil {
			doneCh <- err
			return
		}

		switch x := out.Payload.(type) {
		case *control.SessionOut_HelloApiServer:
			log.Warn("Received duplicate server hello.")
		case *control.SessionOut_ApiServerConfig:
			recvCh <- x.ApiServerConfig
		case *control.SessionOut_DataServers:
			recvCh <- x.DataServers
		case *control.SessionOut_ApiServers:
			recvCh <- x.ApiServers
		case *control.SessionOut_TimestampRequest:
			recvCh <- x.TimestampRequest
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
	case *control.GetApiServers:
		return &control.SessionIn{Payload: &control.SessionIn_GetApiServers{GetApiServers: p}}
	case *control.ApiServerStatus:
		return &control.SessionIn{Payload: &control.SessionIn_ApiServerStatus{ApiServerStatus: p}}
	case *control.TimestampResponse:
		return &control.SessionIn{Payload: &control.SessionIn_TimestampResponse{TimestampResponse: p}}
	default:
		panic(fmt.Sprintf("unhandled SessionIn payload type %T", payload))
	}
}
