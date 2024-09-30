package server

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ utils.Lifecycle       = (*roleDrainer)(nil)
	_ control.ServiceServer = (*roleDrainer)(nil)
)

// roleDrainer will drain all in-flight requests and streams when stopped.
type roleDrainer struct {
	control.UnsafeServiceServer
	inner   role
	drainer *utils.Drainer
}

func newRoleDrainer(inner role) *roleDrainer {
	return &roleDrainer{
		inner: inner,
	}
}

func (s *roleDrainer) Start(ctx context.Context) error {
	s.drainer = utils.NewDrainer(ctx)
	return s.inner.Start(ctx)
}

func (s *roleDrainer) Stop() {
	s.drainer.Stop()
	s.inner.Stop()
}

func (s *roleDrainer) Discover(ctx context.Context, req *control.DiscoverRequest) (*control.DiscoverResponse, error) {
	cctx, cancel := s.drainer.WithDrain(ctx)
	defer cancel()
	return s.inner.Discover(cctx, req)
}

func (s *roleDrainer) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	cctx, cancel := s.drainer.WithDrain(ctx)
	defer cancel()
	return s.inner.Register(cctx, req)
}

func (s *roleDrainer) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
	cctx, cancel := s.drainer.WithDrain(stream.Context())
	defer cancel()

	w := &sessionStreamWrapper{
		BidiStreamingServer: stream,
		cctx:                cctx,
	}

	return s.inner.NewSession(w)
}

type sessionStreamWrapper struct {
	grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]
	cctx context.Context
}

func (w *sessionStreamWrapper) Context() context.Context {
	return w.cctx
}
