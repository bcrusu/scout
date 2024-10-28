package interceptors

import (
	"context"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	logErrors   = logging.New("rpc_errors")
	errInternal = status.Error(codes.Internal, "Internal error")
)

// UnaryErrorsServerInterceptor converts Go errors to gRPC errors
func UnaryErrorsServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		result, err := handler(ctx, req)
		return result, getRPCError(ctx, err, info.FullMethod)
	}
}

// StreamErrorsServerInterceptor converts Go errors to gRPC errors
func StreamErrorsServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		return getRPCError(ss.Context(), err, info.FullMethod)
	}
}

// UnaryErrorsClientInterceptor converts gRPC errors to Go errors
func UnaryErrorsClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		return getGoError(err)
	}
}

// StreamErrorsClientInterceptor converts gRPC errors to Go errors
func StreamErrorsClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		return &csWrapperForErrors{stream}, getGoError(err)
	}
}

type csWrapperForErrors struct {
	grpc.ClientStream
}

func (w *csWrapperForErrors) RecvMsg(m any) error {
	err := w.ClientStream.RecvMsg(m)
	return getGoError(err)
}

func getRPCError(ctx context.Context, err error, method string) error {
	switch err {
	case nil:
		return nil
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, "Deadline exceeded")
	case context.Canceled:
		return status.Error(codes.Canceled, "Canceled")
	case errors.Unavailable:
		return status.Error(codes.Unavailable, "Unavailable")
	case errors.NotLeader:
		return status.Error(codes.Unavailable, "Not Leader")
	case errors.NotFound:
		return status.Error(codes.NotFound, "Not found")
	case errors.InvalidRequest:
		return status.Error(codes.InvalidArgument, "Invalid Request")
	case errors.PermissionDenied:
		return status.Error(codes.PermissionDenied, "Permission Denied")
	case errors.NotRegistered:
		return status.Error(codes.Unauthenticated, "Not Registered")
	case errors.ResourceExhausted:
		return status.Error(codes.ResourceExhausted, "Resource Exhausted")
	case errors.TransactionAborted:
		return status.Error(codes.Aborted, "Transaction Aborted")
	case errors.CorruptedData:
		return status.Error(codes.DataLoss, "Corrupted Data")
	case errors.TimeOffsetOutOfRange:
		return status.Error(codes.OutOfRange, "Time offset out of range")
	case errInternal:
		return errInternal
	}

	switch x := err.(type) {
	case errors.ValidationError:
		return status.Error(codes.InvalidArgument, x.Message)
	}

	logErrors.WithError(err).Error(ctx, "Unhandled error", "method", method)
	return errInternal
}

func getGoError(err error) error {
	if err == nil {
		return nil
	}

	switch s := status.Convert(err); s.Code() {
	case codes.OK:
		return nil
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.Canceled:
		return context.Canceled
	case codes.Unavailable:
		if s.Message() == "Not Leader" {
			return errors.NotLeader
		}
		return errors.Unavailable
	case codes.InvalidArgument:
		if s.Message() == "Invalid Request" {
			return errors.InvalidRequest
		}
		return errors.ValidationError{Message: s.Message()}
	case codes.NotFound:
		return errors.NotFound
	case codes.PermissionDenied:
		return errors.PermissionDenied
	case codes.Unauthenticated:
		if s.Message() == "Not Registered" {
			return errors.NotRegistered
		}
	case codes.ResourceExhausted:
		return errors.ResourceExhausted
	case codes.Aborted:
		if s.Message() == "Transaction Aborted" {
			return errors.TransactionAborted
		}
	case codes.DataLoss:
		if s.Message() == "Corrupted Data" {
			return errors.CorruptedData
		}
	case codes.OutOfRange:
		if s.Message() == "Time offset out of range" {
			return errors.TimeOffsetOutOfRange
		}
	}

	return err
}
