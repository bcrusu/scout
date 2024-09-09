package storage

import (
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

// Marker interface for allowed command payload types.
type Payload interface {
	isPayload()
}

// NewCommand returns a new commmand with the specified payload.
func NewCommand(payload Payload) (*Command, error) {
	var p isCommand_Payload

	switch x := payload.(type) {
	case *Bootstrap:
		p = &Command_Bootstrap{Bootstrap: x}
	case *Register:
		p = &Command_Register{Register: x}
	default:
		return nil, errors.Errorf("newCommand: unhandled payload type %T", payload)
	}

	return &Command{
		Payload: p,
	}, nil
}

// ApplyR applies the command and returns the result or error from FSM.
func ApplyR[T any](raft *multiraft.Raft, payload Payload) (T, error) {
	var zero T
	cmd, err := NewCommand(payload)
	if err != nil {
		return zero, err
	}

	data, err := utils.MarshalProto(cmd)
	if err != nil {
		return zero, err
	}

	result, err := raft.Apply(data)
	if err != nil {
		return zero, err
	}

	if t, ok := result.(T); !ok {
		return zero, errors.Errorf("bad result type from apply: expected %T, got %T", zero, result)
	} else {
		return t, nil
	}
}

// Apply applies the command and returns the error from FSM.
func Apply(raft *multiraft.Raft, payload Payload) error {
	result, err := ApplyR[any](raft, payload)
	if err != nil {
		return err
	} else if result != nil {
		return errors.Errorf("unexpected non-nil response %T from apply", result)
	}
	return nil
}

func getPayload(cmd *Command) (Payload, error) {
	switch x := cmd.Payload.(type) {
	case *Command_Bootstrap:
		return x.Bootstrap, nil
	case *Command_Register:
		return x.Register, nil
	default:
		return nil, errors.Errorf("getPayload: unhandled payload type %T", cmd.Payload)
	}
}

func (*Bootstrap) isPayload() {}
func (*Register) isPayload()  {}
