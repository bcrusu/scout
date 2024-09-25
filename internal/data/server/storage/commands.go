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
	case *TxnBatch:
		p = &Command_TxnBatch{TxnBatch: x}
	default:
		return nil, errors.Errorf("newCommand: unhandled payload type %T", payload)
	}

	return &Command{
		Payload: p,
	}, nil
}

func getPayload(cmd *Command) (payload, error) {
	switch x := cmd.Payload.(type) {
	case *Command_TxnBatch:
		return x.TxnBatch, nil
	default:
		return nil, errors.Errorf("getPayload: unhandled payload type %T", cmd.Payload)
	}
}

func (*TxnBatch) isPayload() {}
