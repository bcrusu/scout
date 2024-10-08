package sessions

import "github.com/bcrusu/scout/internal/control"

type startSession struct {
	stream        sessionStream
	serverID      uint64
	serverAddress string
	waitCh        chan error
}

type sessionMessage interface {
	ID() sessionID
}

type sessionLoopDone struct {
	id  sessionID
	err error
}

type sessionReceived struct {
	id sessionID
}

type dataServerStatus struct {
	id     sessionID
	status *control.DataServerStatus
}

func (m sessionLoopDone) ID() sessionID {
	return m.id
}

func (m sessionReceived) ID() sessionID {
	return m.id
}

func (m dataServerStatus) ID() sessionID {
	return m.id
}
