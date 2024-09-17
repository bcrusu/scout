package server

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ utils.Lifecycle = (*DrainerService)(nil)
)

// DrainerService is the service that will drain all in-flight requests and streams when stopped.
type DrainerService struct {
	control.UnimplementedServiceServer
	inner   control.ServiceServer
	drainer *utils.Drainer
}

// NewDrainerService returns a new DrainerService instance
func NewDrainerService(inner control.ServiceServer) *DrainerService {
	return &DrainerService{
		inner: inner,
	}
}

func (s *DrainerService) Start(ctx context.Context) error {
	s.drainer = utils.NewDrainer(ctx)
	return nil
}

func (s *DrainerService) Stop() {
	s.drainer.Stop()
}

func (s *DrainerService) Discover(ctx context.Context, req *control.DiscoverRequest) (*control.DiscoverResponse, error) {
	cctx, cancel := s.drainer.WithDrain(ctx)
	defer cancel()
	return s.inner.Discover(cctx, req)
}

func (s *DrainerService) Register(ctx context.Context, req *control.RegisterRequest) (*control.RegisterResponse, error) {
	cctx, cancel := s.drainer.WithDrain(ctx)
	defer cancel()
	return s.inner.Register(cctx, req)
}

func (s *DrainerService) NewSession(stream grpc.BidiStreamingServer[control.SessionIn, control.SessionOut]) error {
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
