package leader

import (
	"sync"

	"github.com/bcrusu/graph/internal/control"
)

type startSessionCmd struct {
	stream sessionStream
	hello  *control.SessionIn
	result chan startSessionRes
}

type startSessionRes struct {
	wg  *sync.WaitGroup
	err error
}

type sessionLoopEnd struct {
	session *session
}

type sessionHeartbeat struct {
	session *session
}
