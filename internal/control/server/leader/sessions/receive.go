package sessions

import (
	"io"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
)

func (t *Tracker) sessionRecvLoop(sess *session, stream sessionStream) {
	endSession := func(err error) {
		t.sessionCh <- sessionLoopDone{id: sess.id, err: err}
	}

	for {
		in, err := stream.Recv()
		if err != nil {
			if errors.IsContextError(err) || errors.Is(err, io.EOF) {
				sess.log.WithError(err).Debug("Session receive loop done.")
				endSession(nil)
			} else {
				t.meters.MsgReceiveError.Add(1)
				sess.log.WithError(err).Error("Session receive failed.")
				endSession(err)
			}
			return
		}

		t.meters.MsgReceiveSuccess.Add(1)

		if !sess.recvLimiter.Allow() {
			sess.recvOffenses++
			if sess.recvOffenses == t.config.Sessions.ReceiveMaxOffenses {
				sess.log.Error("Session triggered too many offenses. Closing session.")
				endSession(errors.ResourceExhausted)
				return
			}

			t.meters.MsgReceiveDropped.Add(1)
			sess.log.Errorf("Session triggered receive rate limiter. Dropping message %T.", in.Payload)
			continue
		}

		sess.recvOffenses = 0
		t.sessionCh <- sessionReceived{id: sess.id}

		switch x := in.Payload.(type) {
		case *control.SessionIn_Hello:
			sess.log.Warn("Received duplicate hello.")
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
		case *control.SessionIn_TimestampResponse:
			if err := t.handleTimestampResponse(sess, x.TimestampResponse); err != nil {
				endSession(err)
				return
			}
		default:
			sess.log.Warnf("Unknown session payload type %T", in.Payload)
		}
	}
}

func (t *Tracker) handleHeartbeat(sess *session, msg *control.Heartbeat) error {
	switch sess.serverType {
	case control.ServerType_Data:
		status, ok := msg.Status.(*control.Heartbeat_DataServerStatus)
		if !ok {
			return errors.InvalidRequest
		}

		if msg.ConfigETag != sess.dsConfig.ETag {
			sess.trySend(newSessionOut(sess.dsConfig))
		}

		return t.handleDataServerStatus(sess, status.DataServerStatus)
	case control.ServerType_Api:
		status, ok := msg.Status.(*control.Heartbeat_ApiServerStatus)
		if !ok {
			return errors.InvalidRequest
		}

		if msg.ConfigETag != sess.asConfig.ETag {
			sess.trySend(newSessionOut(sess.asConfig))
		}

		return t.handleApiServerStatus(sess, status.ApiServerStatus)
	}

	return nil
}

func (t *Tracker) handleTimestampResponse(_ *session, msg *control.TimestampResponse) error {
	offset := t.computeTimeOffset(msg)

	if offset > t.config.Sessions.MaxTimeOffset {
		return errors.TimeOutOfRange
	}

	return nil
}

func (t *Tracker) handleGetDataServers(sess *session, msg *control.GetDataServers) error {
	if msg.IfNoMatch != "" && msg.IfNoMatch == t.dataServers.Load().ETag {
		return nil
	}

	ds := t.dataServers.Load()
	sess.trySend(newSessionOut(ds))
	return nil
}

func (t *Tracker) handleGetApiServers(sess *session, msg *control.GetApiServers) error {
	if msg.IfNoMatch != "" && msg.IfNoMatch == t.apiServers.Load().ETag {
		return nil
	}

	as := t.apiServers.Load()
	sess.trySend(newSessionOut(as))
	return nil
}

func (t *Tracker) handleDataServerStatus(sess *session, msg *control.DataServerStatus) error {
	if sess.serverType != control.ServerType_Data {
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

func (t *Tracker) handleApiServerStatus(sess *session, _ *control.ApiServerStatus) error {
	if sess.serverType != control.ServerType_Api {
		return errors.PermissionDenied
	}

	return nil
}

// The offset is computed using the NTP clock synchronization algorithm
// formula: θ = 1/2 * [(t2 − t1) + (t3 − t4)], with the assumption that t2==t3.
func (t *Tracker) computeTimeOffset(msg *control.TimestampResponse) time.Duration {
	t1 := msg.RequestTimestamp.AsTime()
	t2 := msg.ResponseTimestamp.AsTime()
	t3 := t2
	t4 := time.Now()

	offset := (t2.Sub(t1) + t3.Sub(t4)) / 2

	if offset < 0 {
		offset = -offset
	}

	return offset
}
