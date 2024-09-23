package sessions

import (
	"io"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/errors"
)

func (t *Tracker) sessionRecvLoop(sess *session, stream sessionStream) {
	endSession := func(err error) {
		t.sessionCh <- sessionLoopDone{id: sess.id, err: err}
	}

	for {
		in, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				sess.log.WithError(err).Error(sess.ctx, "Session receive failed.")
			} else {
				sess.log.Debug(sess.ctx, "Session receive loop done.")
			}
			endSession(nil)
			return
		}

		if !sess.recvLimiter.Allow() {
			sess.recvOffenses++
			if sess.recvOffenses == recvMaxOffenses {
				sess.log.Error(sess.ctx, "Session triggered too many offenses. Closing session.")
				endSession(errors.ResourceExhausted)
				return
			}
			sess.log.Error(sess.ctx, "Session triggered receive rate limiter. Dropping message.")
			continue
		}

		sess.recvOffenses = 0
		t.sessionCh <- sessionReceived{id: sess.id}

		switch x := in.Payload.(type) {
		case *control.SessionIn_Hello:
			sess.log.Warn(sess.ctx, "Received duplicate hello.")
		case *control.SessionIn_Heartbeat:
			if err := t.handleHeartbeat(sess, x.Heartbeat); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_GetDataServers:
			if err := t.handleGetDataServers(sess, x.GetDataServers); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_GetApiServers:
			if err := t.handleGetApiServers(sess, x.GetApiServers); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_DataServerStatus:
			if err := t.handleDataServerStatus(sess, x.DataServerStatus); err != nil {
				endSession(err)
				return
			}
		case *control.SessionIn_ApiServerStatus:
			if err := t.handleApiServerStatus(sess, x.ApiServerStatus); err != nil {
				endSession(err)
				return
			}
		default:
			sess.log.Warnf(sess.ctx, "Unknown session payload type %T", in.Payload)
		}
	}
}

func (t *Tracker) handleHeartbeat(sess *session, msg *control.Heartbeat) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid Heartbeat.")
		return errors.InvalidRequest
	}

	switch sess.serverType {
	case control.ServerType_Data:
		if msg.ConfigETag != sess.dsConfig.ETag {
			sess.trySend(newSessionOut(sess.dsConfig))
		}
	case control.ServerType_Api:
		if msg.ConfigETag != sess.asConfig.ETag {
			sess.trySend(newSessionOut(sess.asConfig))
		}
	}

	return nil
}

func (t *Tracker) handleGetDataServers(sess *session, msg *control.GetDataServers) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid GetDataServers request.")
		return errors.InvalidRequest
	} else if msg.IfNoMatch != "" && msg.IfNoMatch == t.dataServers.Load().ETag {
		return nil
	}

	ds := t.dataServers.Load()
	sess.trySend(newSessionOut(ds))
	return nil
}

func (t *Tracker) handleGetApiServers(sess *session, msg *control.GetApiServers) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid GetApiServers request.")
		return errors.InvalidRequest
	} else if msg.IfNoMatch != "" && msg.IfNoMatch == t.apiServers.Load().ETag {
		return nil
	}

	as := t.apiServers.Load()
	sess.trySend(newSessionOut(as))
	return nil
}

func (t *Tracker) handleDataServerStatus(sess *session, msg *control.DataServerStatus) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid DataServerStatus request.")
		return errors.InvalidRequest
	} else if sess.serverType != control.ServerType_Data {
		return errors.PermissionDenied
	} else if len(msg.Replicas) == 0 {
		return nil
	}

	count := t.store.PartitionCount()
	for id := range msg.Replicas {
		if id >= count {
			return errors.InvalidRequest
		}
	}

	t.sessionCh <- dataServerStatus{
		id:     sess.id,
		status: msg,
	}

	return nil
}

func (t *Tracker) handleApiServerStatus(sess *session, msg *control.ApiServerStatus) error {
	if err := msg.Validate(); err != nil {
		sess.log.WithError(err).Error(sess.ctx, "Invalid ApiServerStatus request.")
		return errors.InvalidRequest
	} else if sess.serverType != control.ServerType_Api {
		return errors.PermissionDenied
	}

	return nil
}
