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
func UnaryHlcServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := updateHlc(ctx); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamHlcServerInterceptor updates with the incoming Hybrid Logical Clock timestamp.
func StreamHlcServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := updateHlc(ss.Context()); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

// UnaryHlcClientInterceptor sets the outgoing Hybrid Logical Clock timestamp.
func UnaryHlcClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = appendHlc(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamHlcClientInterceptor sets the outgoing Hybrid Logical Clock timestamp.
func StreamHlcClientInterceptor() grpc.StreamClientInterceptor {
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

func updateHlc(ctx context.Context) error {
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
