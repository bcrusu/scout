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
	case *UpdateStatus:
		p = &Command_UpdateStatus{UpdateStatus: x}
	case *InitAssignments:
		p = &Command_InitAssignments{InitAssignments: x}
	case *UpdateAssignments:
		p = &Command_UpdateAssignments{UpdateAssignments: x}
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
	case *Command_UpdateStatus:
		return x.UpdateStatus
	case *Command_InitAssignments:
		return x.InitAssignments
	case *Command_UpdateAssignments:
		return x.UpdateAssignments
	default:
		panic(fmt.Sprintf("unhandled payload type %T", cmd.Payload))
	}
}

func (*Bootstrap) isPayload()         {}
func (*Register) isPayload()          {}
func (*UpdateStatus) isPayload()      {}
func (*InitAssignments) isPayload()   {}
func (*UpdateAssignments) isPayload() {}
