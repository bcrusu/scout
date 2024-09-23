package storage

import (
	"github.com/bcrusu/graph/internal/errors"
)

// Marker interface for allowed command payload types.
type payload interface {
	isPayload()
}

// newCommand returns a new commmand with the specified payload.
func newCommand(payload payload) (*Command, error) {
	var p isCommand_Payload

	switch x := payload.(type) {
	case *Bootstrap:
		p = &Command_Bootstrap{Bootstrap: x}
	case *Register:
		p = &Command_Register{Register: x}
	case *UpdateServerStatus:
		p = &Command_UpdateServerStatus{UpdateServerStatus: x}
	case *UpdatePartitionStatus:
		p = &Command_UpdatePartitionStatus{UpdatePartitionStatus: x}
	default:
		return nil, errors.Errorf("newCommand: unhandled payload type %T", payload)
	}

	return &Command{
		Payload: p,
	}, nil
}

func getPayload(cmd *Command) (payload, error) {
	switch x := cmd.Payload.(type) {
	case *Command_Bootstrap:
		return x.Bootstrap, nil
	case *Command_Register:
		return x.Register, nil
	case *Command_UpdateServerStatus:
		return x.UpdateServerStatus, nil
	case *Command_UpdatePartitionStatus:
		return x.UpdatePartitionStatus, nil
	default:
		return nil, errors.Errorf("getPayload: unhandled payload type %T", cmd.Payload)
	}
}

func (*Bootstrap) isPayload()             {}
func (*Register) isPayload()              {}
func (*UpdateServerStatus) isPayload()    {}
func (*UpdatePartitionStatus) isPayload() {}
