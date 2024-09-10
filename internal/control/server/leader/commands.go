package leader

import (
	"github.com/bcrusu/graph/internal/control/server/storage"
)

type command struct {
	payload  any
	resultCh chan error
}

type sessionStarting struct {
	server  *storage.Server
	address string
	session *session
}

type sessionEnded struct {
	server  *storage.Server
	session *session
}
