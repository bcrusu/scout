package sessions

import (
	"fmt"
	"io"

	"github.com/bcrusu/graph/internal/control"
)

func (t *Tracker) sessionSendLoop(sess *session, stream sessionStream) {
	// enqueue server hello
	if out := t.makeServerHello(sess); out != nil {
		sess.sendBufferCh <- out
	}

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

		t.sessionCh <- sessionLoopDone{id: sess.id, err: nil}
		return
	}
}

func (t *Tracker) makeServerHello(sess *session) *control.SessionOut {
	var out *control.SessionOut

	switch sess.serverType {
	case control.ServerType_Data:
		out = newSessionOut(&control.HelloDataServer{
			Config:      sess.dsConfig,
			DataServers: t.dataServers.Load().(*control.DataServers),
		})
	case control.ServerType_Api:
		out = newSessionOut(&control.HelloApiServer{
			Config:      sess.asConfig,
			DataServers: t.dataServers.Load().(*control.DataServers),
			ApiServers:  t.apiServers.Load().(*control.ApiServers),
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
	default:
		panic(fmt.Sprintf("unhandled SessionOut payload type %T", payload))
	}
}
