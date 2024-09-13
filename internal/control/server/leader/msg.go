package leader

type startSession struct {
	stream        sessionStream
	serverID      serverID
	serverAddress string
	waitCh        chan error
}

type sessionMessage interface {
	ID() sessionID
}

type endSession struct {
	id  sessionID
	err error
}

type sessionReceived struct {
	id sessionID
}

type updateLeader struct {
	id          sessionID
	currentTerm map[partitionID]uint64
}

func (m endSession) ID() sessionID {
	return m.id
}

func (m sessionReceived) ID() sessionID {
	return m.id
}

func (m updateLeader) ID() sessionID {
	return m.id
}
