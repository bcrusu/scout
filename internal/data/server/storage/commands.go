package storage

import (
	"fmt"
)

// cmdPayload is a marker interface for allowed command payloads
type cmdPayload interface {
	isPayload()
}

// NewCommand returns a new commmand with the specified payload.
func newCommand(payload cmdPayload) *Command {
	var p isCommand_Payload

	switch x := payload.(type) {
	case *TxnBatch:
		p = &Command_Batch{Batch: x}
	default:
		panic(fmt.Sprintf("unhandled payload type %T", payload))
	}

	return &Command{Payload: p}
}

func getPayload(cmd *Command) cmdPayload {
	switch x := cmd.Payload.(type) {
	case *Command_Batch:
		return x.Batch
	default:
		panic(fmt.Sprintf("unhandled payload type %T", cmd.Payload))
	}
}

func (*TxnBatch) isPayload() {}
