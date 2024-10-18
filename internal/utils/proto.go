package utils

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type protoMessage[T any] interface {
	proto.Message
	*T
}

// MarshalProto returns the proto bytes.
func MarshalProto(msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal failed for %T", msg)
	}

	return data, nil
}

// UnmarshalProto returns the message for the provided bytes.
func UnmarshalProto[T any, P protoMessage[T]](b []byte) (p P, err error) {
	p = new(T)
	if err := proto.Unmarshal(b, p); err != nil {
		return *new(P), err
	}
	return p, nil
}

func TimeMapToProto[T comparable](in map[T]time.Time) map[T]*timestamppb.Timestamp {
	if in == nil {
		return nil
	}

	out := map[T]*timestamppb.Timestamp{}
	for k, v := range in {
		out[k] = timestamppb.New(v)
	}

	return out
}

func TimeMapFromProto[T comparable](in map[T]*timestamppb.Timestamp) map[T]time.Time {
	if in == nil {
		return nil
	}

	out := map[T]time.Time{}
	for k, v := range in {
		out[k] = v.AsTime()
	}

	return out
}

func CloneProto[T proto.Message](orig T) T {
	clone := proto.Clone(orig)
	t, ok := clone.(T)
	if !ok {
		panic("CloneProto failed")
	}
	return t
}

func CloneProtoMap[K comparable, V proto.Message](orig map[K]V) map[K]V {
	result := map[K]V{}

	for k, v := range orig {
		clone := proto.Clone(v)
		v, ok := clone.(V)
		if !ok {
			panic("CloneProto failed")
		}
		result[k] = v
	}

	return result
}

func CloneProtoMapValues[K comparable, V proto.Message](orig map[K]V) []V {
	result := make([]V, 0, len(orig))

	for _, v := range orig {
		clone := proto.Clone(v)
		v, ok := clone.(V)
		if !ok {
			panic("CloneProto failed")
		}
		result = append(result, v)
	}

	return result
}
