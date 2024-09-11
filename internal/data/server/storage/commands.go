package storage

import (
	"github.com/bcrusu/graph/internal/errors"
)

// Payload is a marker interface for allowed command payloads
type payload interface {
	isPayload()
}

// NewCommand returns a new commmand with the specified payload.
func newCommand(payload payload) (*Command, error) {
	var p isCommand_Payload

	switch x := payload.(type) {
	case *Set:
		p = &Command_Set{Set: x}
	case *Delete:
		p = &Command_Delete{Delete: x}
	default:
		return nil, errors.Errorf("newCommand: unhandled payload type %T", payload)
	}

	return &Command{
		Payload: p,
	}, nil
}

func getPayload(cmd *Command) (payload, error) {
	switch x := cmd.Payload.(type) {
	case *Command_Set:
		return x.Set, nil
	case *Command_Delete:
		return x.Delete, nil
	default:
		return nil, errors.Errorf("getPayload: unhandled payload type %T", cmd.Payload)
	}
}

func (*Set) isPayload()    {}
func (*Delete) isPayload() {}
