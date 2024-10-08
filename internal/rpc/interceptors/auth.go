package interceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	errUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")
)

// UnaryAuthServerInterceptor checks that incoming requests can be authenticated.
func UnaryAuthServerInterceptor(clusterName string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkAuth(ctx, clusterName); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamAuthServerInterceptor checks that incoming requests can be authenticated.
func StreamAuthServerInterceptor(clusterName string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkAuth(ss.Context(), clusterName); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

// UnaryAuthClientInterceptor sets the
func UnaryAuthClientInterceptor(clusterName string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = appendAuth(ctx, clusterName)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamAuthClientInterceptor converts gRPC errors to Go errors
func StreamAuthClientInterceptor(clusterName string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = appendAuth(ctx, clusterName)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func appendAuth(ctx context.Context, clusterName string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "scout-cluster-name", clusterName)
}

func checkAuth(ctx context.Context, clusterName string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errUnauthenticated
	}

	incoming, ok := md["scout-cluster-name"]
	if !ok || len(incoming) != 1 || incoming[0] != clusterName {
		return errUnauthenticated
	}

	return nil
}
