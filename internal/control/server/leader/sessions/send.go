package sessions

import (
	"fmt"
	"io"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (t *Tracker) sessionSendLoop(sess *session, stream sessionStream) {
	timestampTicker := time.NewTicker(utils.AddJitter(t.config.Sessions.TimeOffsetCheckInterval))
	defer timestampTicker.Stop()

	// enqueue server hello and all the other things a new session might need to live long and prosper
	switch sess.serverType {
	case control.ServerType_Control:
		sess.sendBufferCh <- t.newServerHello()
	case control.ServerType_Data:
		sess.sendBufferCh <- t.newServerHello()
		sess.sendBufferCh <- newSessionOut(sess.dsConfig)
		sess.sendBufferCh <- newSessionOut(t.dataServers.Load())
	case control.ServerType_Api:
		sess.sendBufferCh <- t.newServerHello()
		sess.sendBufferCh <- newSessionOut(sess.asConfig)
		sess.sendBufferCh <- newSessionOut(t.dataServers.Load())
		sess.sendBufferCh <- newSessionOut(t.apiServers.Load())
	}

	for {
		select {
		case out := <-sess.sendBufferCh:
			err := stream.Send(out)

			switch {
			case err == nil:
				t.meters.MsgSendSuccess.Add(1)
				continue
			case errors.IsContextError(err) || errors.Is(err, io.EOF):
				sess.log.WithError(err).Trace("Session send loop done.")
			default:
				t.meters.MsgSendError.Add(1)
				sess.log.WithError(err).Error("Session send failed.")
			}

			t.sessionCh <- sessionLoopDone{id: sess.id, err: nil}
			return
		case <-timestampTicker.C:
			out := newSessionOut(&control.TimestampRequest{
				RequestTimestamp: timestamppb.Now(),
			})

			// enqueue the message to use the same error handling code above
			sess.sendBufferCh <- out
		}
	}
}

func (t *Tracker) newServerHello() *control.SessionOut {
	return newSessionOut(&control.HelloResponse{
		HlcTimestamp: hlc.Now(),
	})
}

func (s *session) trySend(out *control.SessionOut) {
	select {
	case s.sendBufferCh <- out:
	default:
		s.tracker.meters.MsgSendDropped.Add(1)
		s.log.Warnf("Session send buffer is full. Message %T was dropped.", out)
	}
}

func (s sessions) trySendAll(out *control.SessionOut) {
	for _, sess := range s {
		sess.trySend(out)
	}
}

func (s sessions) trySendServerType(out *control.SessionOut, serverType control.ServerType) {
	for _, sess := range s {
		if sess.serverType == serverType {
			sess.trySend(out)
		}
	}
}

func newSessionOut(payload any) *control.SessionOut {
	switch p := payload.(type) {
	case *control.HelloResponse:
		return &control.SessionOut{Payload: &control.SessionOut_Hello{Hello: p}}
	case *control.DataServerConfig:
		return &control.SessionOut{Payload: &control.SessionOut_DataServerConfig{DataServerConfig: p}}
	case *control.ApiServerConfig:
		return &control.SessionOut{Payload: &control.SessionOut_ApiServerConfig{ApiServerConfig: p}}
	case *control.DataServers:
		return &control.SessionOut{Payload: &control.SessionOut_DataServers{DataServers: p}}
	case *control.ApiServers:
		return &control.SessionOut{Payload: &control.SessionOut_ApiServers{ApiServers: p}}
	case *control.TimestampRequest:
		return &control.SessionOut{Payload: &control.SessionOut_TimestampRequest{TimestampRequest: p}}
	default:
		panic(fmt.Sprintf("unhandled SessionOut payload type %T", payload))
	}
}
