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
	timestampTicker := time.NewTicker(utils.AddJitter(t.config.Sessions.TimeOffsetCheckInterval, 0.10))
	defer timestampTicker.Stop()

	// enqueue server hello
	if out := t.makeServerHello(sess); out != nil {
		sess.sendBufferCh <- out
	}

	for {
		select {
		case out := <-sess.sendBufferCh:
			err := stream.Send(out)
			if err == nil {
				continue
			}

			if !errors.Is(err, io.EOF) {
				sess.log.WithError(err).Error(sess.ctx, "Session send failed.")
			} else {
				sess.log.Debug(sess.ctx, "Session send loop done.")
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

func (t *Tracker) makeServerHello(sess *session) *control.SessionOut {
	var out *control.SessionOut

	switch sess.serverType {
	case control.ServerType_Data:
		out = newSessionOut(&control.HelloDataServer{
			Timestamp:   hlc.Now(),
			Config:      sess.dsConfig,
			DataServers: t.dataServers.Load(),
		})
	case control.ServerType_Api:
		out = newSessionOut(&control.HelloApiServer{
			Timestamp:   hlc.Now(),
			Config:      sess.asConfig,
			DataServers: t.dataServers.Load(),
			ApiServers:  t.apiServers.Load(),
		})
	}

	return out
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
	case *control.TimestampRequest:
		return &control.SessionOut{Payload: &control.SessionOut_TimestampRequest{TimestampRequest: p}}
	default:
		panic(fmt.Sprintf("unhandled SessionOut payload type %T", payload))
	}
}
