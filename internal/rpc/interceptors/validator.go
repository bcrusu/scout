package interceptors

import (
	"context"
	"reflect"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/validation"
	"google.golang.org/grpc"
)

// UnaryValidatorServerInterceptor validates the incoming requests.
func UnaryValidatorServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := validateMessage(ctx, req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamValidatorServerInterceptor validates the incoming stream messages.
func StreamValidatorServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		sw := &ssWrapperForValidator{ServerStream: ss}
		return handler(srv, sw)
	}
}

// UnaryValidatorClientInterceptor validates the incoming server reply.
func UnaryValidatorClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return err
		} else if err := validateMessage(ctx, reply); err != nil {
			return err
		}
		return nil
	}
}

// StreamValidatorClientInterceptor validates the incoming stream messages.
func StreamValidatorClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &csWrapperForValidator{ClientStream: cs}, nil
	}
}

type ssWrapperForValidator struct {
	grpc.ServerStream
}

type csWrapperForValidator struct {
	grpc.ClientStream
}

func (s *ssWrapperForValidator) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	} else if err := validateMessage(s.Context(), m); err != nil {
		return err
	}
	return nil
}

func (s *csWrapperForValidator) RecvMsg(m any) error {
	if err := s.ClientStream.RecvMsg(m); err != nil {
		return err
	} else if err := validateMessage(s.Context(), m); err != nil {
		return err
	}
	return nil
}

func validateMessage(ctx context.Context, value any) error {
	if reflect.ValueOf(value).IsNil() {
		logging.Error(ctx, "Nil message %T.", value)
		return errors.InvalidRequest
	}

	v, ok := value.(validation.CanValidate)
	if !ok {
		logging.Debug(ctx, "Message %T does not implement validation.", value)
		return nil
	}

	err := v.Validate()
	if err != nil {
		logging.WithError(err).Error(ctx, "Invalid message %T.", value)
		return errors.InvalidRequest
	}

	return nil
}
