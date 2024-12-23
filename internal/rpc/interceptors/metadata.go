package interceptors

import (
	"context"

	"github.com/bcrusu/scout/internal/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// UnaryMetadataServerInterceptor converts Go errors to gRPC errors
func UnaryMetadataServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = extractMetadata(ctx)
		return handler(ctx, req)
	}
}

// StreamMetadataServerInterceptor converts Go errors to gRPC errors
func StreamMetadataServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ss = &ssWrapperForMetadata{
			ServerStream: ss,
			ctx:          extractMetadata(ss.Context()),
		}

		return handler(srv, ss)
	}
}

// UnaryMetadataClientInterceptor converts gRPC errors to Go errors
func UnaryMetadataClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = appendMetadata(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamMetadataClientInterceptor converts gRPC errors to Go errors
func StreamMetadataClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = appendMetadata(ctx)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func appendMetadata(ctx context.Context) context.Context {
	if tracing.GetTraceID(ctx) == "" {
		ctx = tracing.WithNewTraceID(ctx)
	}

	var kv []string

	if value := tracing.GetTraceID(ctx); value != "" {
		kv = append(kv, "scout-trace-id", value)
	}

	return metadata.AppendToOutgoingContext(ctx, kv...)
}

func extractMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return tracing.WithNewTraceID(ctx)
	}

	if values, ok := md["scout-trace-id"]; ok {
		ctx = tracing.WithTraceID(ctx, values[0])
	} else {
		ctx = tracing.WithNewTraceID(ctx)
	}

	return ctx
}

type ssWrapperForMetadata struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ssWrapperForMetadata) Context() context.Context {
	return s.ctx
}
