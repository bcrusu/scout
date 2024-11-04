package interceptors

import (
	"context"
	"runtime"

	"github.com/bcrusu/scout/internal/logging"
	"google.golang.org/grpc"
)

var (
	logRecovery = logging.New("rpc_recovery")
)

// UnaryRecoveryServerInterceptor returns a new interceptor for panic recovery.
func UnaryRecoveryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ctx, r)
			}
		}()

		return handler(ctx, req)
	}
}

// StreamRecoveryServerInterceptor returns a new interceptor for panic recovery.
func StreamRecoveryServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ss.Context(), r)
			}
		}()

		return handler(srv, ss)
	}
}

func handlePanic(ctx context.Context, p any) error {
	stack := make([]byte, 64<<10)
	stack = stack[:runtime.Stack(stack, false)]

	logRecovery.WithContext(ctx).Error("Recovered.", "panic", p, "stack", stack)
	return errInternal
}
