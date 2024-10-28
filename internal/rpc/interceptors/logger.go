package interceptors

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultLogLevel = logging.LevelDebug
)

var (
	logLogger     = logging.New("rpc_logger")
	streamCounter = &atomic.Uint64{}
	logLevels     = map[codes.Code]logging.Level{
		codes.OK: logging.LevelTrace,
	}
)

// UnaryLoggerServerInterceptor logs unary server calls.
func UnaryLoggerServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		startTime := time.Now()
		resp, err := handler(ctx, req)

		if level := getLevelForError(err); logLogger.Enabled(level) {
			args := []any{
				"method", info.FullMethod,
				"code", status.Code(err),
				"elapsed", time.Since(startTime),
			}

			logLogger.WithError(err).Log(ctx, level, "Handled", args...)
		}

		return resp, err
	}
}

// StreamLoggerServerInterceptor logs streaming server calls.
func StreamLoggerServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		startTime := time.Now()
		wrapper := &ssWrapperForLogger{
			ServerStream: ss,
			method:       info.FullMethod,
			id:           streamCounter.Add(1),
		}

		err := handler(srv, wrapper)

		if level := getLevelForError(err); logLogger.Enabled(level) {
			args := []any{
				"stream", wrapper.id,
				"method", info.FullMethod,
				"code", status.Code(err),
				"elapsed", time.Since(startTime),
			}

			logLogger.WithError(err).Log(ss.Context(), level, "Handled", args...)
		}

		return err
	}
}

// UnaryLoggerClientInterceptor logs unary client calls.
func UnaryLoggerClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		startTime := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)

		if level := getLevelForError(err); logLogger.Enabled(level) {
			args := []any{
				"method", method,
				"code", status.Code(err),
				"elapsed", time.Since(startTime),
			}

			logLogger.WithError(err).Log(ctx, level, "Invoked", args...)
		}

		return err
	}
}

// StreamLoggerClientInterceptor logs streaming client calls.
func StreamLoggerClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		startTime := time.Now()
		id := streamCounter.Add(1)
		cs, err := streamer(ctx, desc, cc, method, opts...)

		if level := getLevelForError(err); logLogger.Enabled(level) {
			args := []any{
				"stream", id,
				"method", method,
				"code", status.Code(err),
				"elapsed", time.Since(startTime),
			}

			logLogger.WithError(err).Log(ctx, level, "Invoked", args...)
		}

		if err != nil {
			return nil, err
		}

		return &csWrapperForLogger{
			ClientStream: cs,
			method:       method,
			id:           id,
		}, nil
	}
}

type ssWrapperForLogger struct {
	grpc.ServerStream
	method string
	id     uint64
}

type csWrapperForLogger struct {
	grpc.ClientStream
	method string
	id     uint64
}

func (s *ssWrapperForLogger) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)

	if level := getLevelForError(err); logLogger.Enabled(level) {
		args := []any{
			"stream", s.id,
			"method", s.method,
			"code", status.Code(err),
		}

		logLogger.WithError(err).Log(s.Context(), level, "Sent", args...)
	}

	return err
}

func (s *ssWrapperForLogger) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)

	if level := getLevelForError(err); logLogger.Enabled(level) {
		args := []any{
			"stream", s.id,
			"method", s.method,
			"code", status.Code(err),
		}

		logLogger.WithError(err).Log(s.Context(), level, "Received", args...)
	}

	return err
}

func (s *csWrapperForLogger) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)

	if level := getLevelForError(err); logLogger.Enabled(level) {
		args := []any{
			"stream", s.id,
			"method", s.method,
			"code", status.Code(err),
		}

		logLogger.WithError(err).Log(s.Context(), level, "Sent", args...)
	}

	return err
}

func (s *csWrapperForLogger) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)

	if level := getLevelForError(err); logLogger.Enabled(level) {
		args := []any{
			"stream", s.id,
			"method", s.method,
			"code", status.Code(err),
		}

		logLogger.WithError(err).Log(s.Context(), level, "Received", args...)
	}

	return err
}

func getLevelForError(err error) logging.Level {
	code := status.Code(err)

	if level, ok := logLevels[code]; ok {
		return level
	}

	return defaultLogLevel
}
