package session

import (
	"context"
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
	_                 utils.Lifecycle = (*Session)(nil)
	heartbeatInterval                 = utils.AddJitter(8*time.Second, 0.2) // TODO: make configurable
	requestChSize                     = 100
	log                               = logging.WithComponent("data_session").NoContext()

	retryBackoff = &utils.Backoff{
		MinDelay: 3 * time.Second,
		MaxDelay: 10 * time.Second,
	}
)

type Session struct {
	client     client.ControlClient
	raft       *multiraft.MultiRaft
	id         identity.Identity
	address    string
	requestCh  chan *control.SessionIn
	cancelFunc context.CancelFunc
}

type stream = grpc.BidiStreamingClient[control.SessionIn, control.SessionOut]

func New(client client.ControlClient, raft *multiraft.MultiRaft, id identity.Identity, address string) *Session {
	return &Session{
		client:    client,
		raft:      raft,
		id:        id,
		address:   address,
		requestCh: make(chan *control.SessionIn, requestChSize),
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

func (m *Session) mainLoop(ctx context.Context) {
	for {
		streamCtx, streamCancel := context.WithCancel(ctx)
		stream, err := m.newStreamWithRetry(streamCtx)
		if err != nil {
			streamCancel()
			return
		}

		doneCh := make(chan error)
		go m.sendLoop(stream, doneCh)
		go m.recvLoop(stream, doneCh)

		select {
		case err := <-doneCh:
			streamCancel()
			<-doneCh // wait for partner

			if err != nil && !errors.Is(err, io.EOF) {
				log.WithError(err).Warn("Session stream ended abruptly. Reconnecting...")
			} else {
				log.Debug("Session stream ended. Reconnecting...")
			}

			// TODO: add some random backoff
		case <-ctx.Done():
			streamCancel()
			<-doneCh
			<-doneCh
			return
		}
	}
}

func (m *Session) sendLoop(stream stream, doneCh chan<- error) {
	var last time.Time
	send := func(in *control.SessionIn, what string) bool {
		if err := stream.Send(in); err != nil {
			doneCh <- errors.Wrapf(err, "send %s failed", what)
			return false
		}
		last = time.Now()
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
		case in := <-m.requestCh:
			if !send(in, "request") {
				return
			}
		case <-ticker.C:
			if time.Now().After(last.Add(heartbeatInterval)) && !send(heartbeat, "heartbeat") {
				return
			}
		}
	}
}

func (m *Session) recvLoop(stream stream, doneCh chan<- error) {
	for {
		_, err := stream.Recv()
		if err != nil {
			doneCh <- err
			return
		}

		// TODO: handle response
	}
}

func (m *Session) newHello() *control.SessionIn {
	return &control.SessionIn{
		ClusterName: m.id.ClusterName,
		ServerId:    m.id.ID,
		Address:     m.address,
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
