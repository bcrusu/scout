package sessions

type startSession struct {
	stream        sessionStream
	serverID      serverID
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

type sessionPartStatus struct {
	id       sessionID
	leader   map[partitionID]uint64 // map[partition_id]raft_leader_term
	follower map[partitionID]bool
}

func (m sessionLoopDone) ID() sessionID {
	return m.id
}

func (m sessionReceived) ID() sessionID {
	return m.id
}

func (m sessionPartStatus) ID() sessionID {
	return m.id
}
