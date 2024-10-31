package interceptors

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryPassthroughServerInterceptor forwards the call to the next interceptor.
func UnaryPassthroughServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(ctx, req)
}

// StreamPassthroughServerInterceptor forwards the call to the next interceptor.
func StreamPassthroughServerInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}

// UnaryPassthroughClientInterceptor forwards the call to the next interceptor.
func UnaryPassthroughClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(ctx, method, req, reply, cc, opts...)
}

// StreamPassthroughClientInterceptor forwards the call to the next interceptor.
func StreamPassthroughClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(ctx, desc, cc, method, opts...)
}
