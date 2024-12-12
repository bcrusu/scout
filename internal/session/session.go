package session

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

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
	log                                        = logging.New("session")
)

type Config struct {
	NewSessionThrottle time.Duration `yaml:"newSessionThrottle" default:"3s" validate:"min:100ms"`
	MaxTimeOffset      time.Duration `yaml:"maxTimeOffset" default:"1s" validate:"min:10ms"`
	HeartbeatInterval  time.Duration `yaml:"heartbeatInterval" default:"5s" validate:"min:100ms"`
	SendBufferSize     int           `yaml:"sendBufferSize" default:"16" validate:"min:1"`
	Address            string        `yaml:"-"`
}

type Session struct {
	config          Config
	id              identity.Identity
	client          client.ControlClient
	cancelFunc      context.CancelFunc
	statusCallback  StatusCallback
	configETag      etag
	dataServersETag etag
	apiServersETag  etag
}

type StatusCallback func() any

type etag struct {
	ptr atomic.Pointer[string]
}

type stream = grpc.BidiStreamingClient[control.SessionIn, control.SessionOut]

func New(id identity.Identity, config Config, client client.ControlClient) *Session {
	return &Session{
		config: config,
		id:     id,
		client: client,
	}
}

func (m *Session) Start(ctx context.Context) error {
	m.cancelFunc = utils.RunAsync(ctx, m.mainLoop)
	return nil
}

func (m *Session) Stop() {
	m.cancelFunc()
}

func (m *Session) SetStatusCallback(cb StatusCallback) {
	m.statusCallback = cb
}

func (m *Session) mainLoop(ctx context.Context) {
	for {
		err := m.runSessionStream(ctx)

		switch {
		case errors.Is(err, io.EOF):
			log.Debug("Session stream ended. Reconnecting...")
		case errors.Is(err, errors.TimeOffsetOutOfRange):
			utils.GracefulShutdown("Time offset is out of allowed range.")
			return
		case errors.IsAny(err, context.Canceled, context.DeadlineExceeded):
			// pass
		case err != nil:
			log.WithError(err).Warn("Session stream ended abruptly. Reconnecting...")
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
	refreshDataServersSub := eventbus.SubscribeThrottled[refreshDataServers](ctx, refreshDataServersThrottle)
	heartbeatTicker := time.NewTicker(utils.AddJitter(m.config.HeartbeatInterval))
	defer refreshDataServersSub.Unsubscribe()
	defer heartbeatTicker.Stop()

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
			if x := m.newHeartbeat(); x != nil {
				sendCh <- x
			}
		case <-refreshDataServersSub.Items():
			if !m.dataServersETag.IsEmpty() {
				sendCh <- m.newSessionIn(&control.GetDataServers{
					IfNoMatch: m.dataServersETag.Get(),
				})
			}
		case msg := <-recvCh:
			switch x := msg.(type) {
			case *control.HelloResponse:
				if err := hlc.Update(x.HlcTimestamp); err != nil {
					log.WithError(err).Error("Failed to update HLC with hello timestamp.")
					closeSession(nil)
					return err
				}
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
			log.Tracef("Sending message %T.", in.Payload)

			if err := stream.Send(in); err != nil {
				doneCh <- err
				return
			}
		case <-stream.Context().Done():
			doneCh <- stream.Context().Err()
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

	payload, ok := out.Payload.(*control.SessionOut_Hello)
	if !ok {
		doneCh <- errors.Error("server did not send hello.")
		return
	}

	log.Trace("Received hello.")

	recvCh <- payload.Hello

	for {
		out, err := stream.Recv()
		if err != nil {
			doneCh <- err
			return
		}

		log.Tracef("Received message %T.", out.Payload)

		switch x := out.Payload.(type) {
		case *control.SessionOut_Hello:
			log.Warn("Received duplicate server hello.")
		case *control.SessionOut_DataServerConfig:
			publish(&m.configETag, x.DataServerConfig.ETag, x.DataServerConfig)
		case *control.SessionOut_ApiServerConfig:
			publish(&m.configETag, x.ApiServerConfig.ETag, x.ApiServerConfig)
		case *control.SessionOut_DataServers:
			publish(&m.dataServersETag, x.DataServers.ETag, x.DataServers)
		case *control.SessionOut_ApiServers:
			publish(&m.apiServersETag, x.ApiServers.ETag, x.ApiServers)
		case *control.SessionOut_TimestampRequest:
			recvCh <- x.TimestampRequest
		default:
			log.Warnf("Unknown session payload type %T", out.Payload)
		}
	}
}

func (m *Session) newHello() *control.SessionIn {
	return m.newSessionIn(&control.HelloRequest{
		ServerId: m.id.ServerID,
		Address:  m.config.Address,
	})
}

func (m *Session) newHeartbeat() *control.SessionIn {
	if m.statusCallback == nil {
		log.Warn("Status callback not set.")
		return nil
	} else if m.configETag.IsEmpty() {
		return nil
	}

	switch x := m.statusCallback().(type) {
	case *control.ControlServerStatus:
		return m.newSessionIn(&control.Heartbeat{
			ConfigETag: m.configETag.Get(),
			Status:     &control.Heartbeat_ControlServerStatus{ControlServerStatus: x},
		})
	case *control.DataServerStatus:
		return m.newSessionIn(&control.Heartbeat{
			ConfigETag: m.configETag.Get(),
			Status:     &control.Heartbeat_DataServerStatus{DataServerStatus: x},
		})
	case *control.ApiServerStatus:
		return m.newSessionIn(&control.Heartbeat{
			ConfigETag: m.configETag.Get(),
			Status:     &control.Heartbeat_ApiServerStatus{ApiServerStatus: x},
		})
	default:
		log.Warnf("Unknown server status type %T", x)
		return nil
	}
}

func (m *Session) newSessionIn(payload any) *control.SessionIn {
	switch p := payload.(type) {
	case *control.HelloRequest:
		return &control.SessionIn{Payload: &control.SessionIn_Hello{Hello: p}}
	case *control.Heartbeat:
		return &control.SessionIn{Payload: &control.SessionIn_Heartbeat{Heartbeat: p}}
	case *control.GetDataServers:
		return &control.SessionIn{Payload: &control.SessionIn_GetDataServers{GetDataServers: p}}
	case *control.GetApiServers:
		return &control.SessionIn{Payload: &control.SessionIn_GetApiServers{GetApiServers: p}}
	case *control.TimestampResponse:
		return &control.SessionIn{Payload: &control.SessionIn_TimestampResponse{TimestampResponse: p}}
	default:
		panic(fmt.Sprintf("unhandled SessionIn payload type %T", payload))
	}
}

func (t *etag) Get() string {
	if x := t.ptr.Load(); x != nil {
		return *x
	}
	return ""
}

func (t *etag) IsEmpty() bool {
	return t.Get() == ""
}

func (t *etag) Set(value string) {
	t.ptr.Store(&value)
}

func publish[T any](etag *etag, newETag string, msg T) {
	if oldETag := etag.Get(); oldETag == newETag {
		return
	}

	eventbus.TryPublish(msg)
	etag.Set(newETag)
}
