package leader

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"google.golang.org/grpc"
)

type session struct {
	stream sessionStream
}

type sessionStream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]

func newSession(stream sessionStream) *session {
	return &session{
		stream: stream,
	}
}

func (s *session) run(ctx context.Context) error {
	return nil
}

func (s *session) recvLoop() {

}

func (s *session) close() {

}
