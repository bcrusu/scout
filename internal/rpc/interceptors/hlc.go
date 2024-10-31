package interceptors

import (
	"context"
	"strconv"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// UnaryHlcServerInterceptor updates with the incoming Hybrid Logical Clock timestamp.
func UnaryHlcServerInterceptor(enabled bool) grpc.UnaryServerInterceptor {
	if !enabled {
		return UnaryPassthroughServerInterceptor
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := updateHlc(ctx, info.FullMethod); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamHlcServerInterceptor updates with the incoming Hybrid Logical Clock timestamp.
func StreamHlcServerInterceptor(enabled bool) grpc.StreamServerInterceptor {
	if !enabled {
		return StreamPassthroughServerInterceptor
	}

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := updateHlc(ss.Context(), info.FullMethod); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

// UnaryHlcClientInterceptor sets the outgoing Hybrid Logical Clock timestamp.
func UnaryHlcClientInterceptor(enabled bool) grpc.UnaryClientInterceptor {
	if !enabled {
		return UnaryPassthroughClientInterceptor
	}

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = appendHlc(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamHlcClientInterceptor sets the outgoing Hybrid Logical Clock timestamp.
func StreamHlcClientInterceptor(enabled bool) grpc.StreamClientInterceptor {
	if !enabled {
		return StreamPassthroughClientInterceptor
	}

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = appendHlc(ctx)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func appendHlc(ctx context.Context) context.Context {
	kv := []string{
		"scout-hlc", strconv.FormatUint(hlc.Now(), 10),
	}

	return metadata.AppendToOutgoingContext(ctx, kv...)
}

func updateHlc(ctx context.Context, fullMethod string) error {
	if isAdmin(fullMethod) {
		return nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errors.ValidationError{Message: "missing hlc timestamp"}
	}

	values, ok := md["scout-hlc"]
	if !ok {
		return errors.ValidationError{Message: "missing hlc timestamp"}
	}

	timestamp, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		return errors.ValidationError{Message: "invalid hlc timestamp"}
	}

	return hlc.Update(timestamp)
}
