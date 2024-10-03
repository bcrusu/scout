package storage

import (
	"fmt"
)

// Marker interface for allowed command payload types.
type payload interface {
	isPayload()
}

// newCommand returns a new commmand with the specified payload.
func newCommand(payload payload) *Command {
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
		panic(fmt.Sprintf("unhandled payload type %T", payload))
	}

	return &Command{Payload: p}
}

func getPayload(cmd *Command) payload {
	switch x := cmd.Payload.(type) {
	case *Command_Bootstrap:
		return x.Bootstrap
	case *Command_Register:
		return x.Register
	case *Command_UpdateServerStatus:
		return x.UpdateServerStatus
	case *Command_UpdatePartitionStatus:
		return x.UpdatePartitionStatus
	default:
		panic(fmt.Sprintf("unhandled payload type %T", cmd.Payload))
	}
}

func (*Bootstrap) isPayload()             {}
func (*Register) isPayload()              {}
func (*UpdateServerStatus) isPayload()    {}
func (*UpdatePartitionStatus) isPayload() {}
