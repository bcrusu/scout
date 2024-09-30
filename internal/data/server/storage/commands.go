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
	case *TxnAutocommit:
		p = &Command_Autocommit{Autocommit: x}
	case *TxnPrepare:
		p = &Command_Prepare{Prepare: x}
	case *TxnCommit:
		p = &Command_Commit{Commit: x}
	case *TxnAbort:
		p = &Command_Abort{Abort: x}
	case *StoreTxnDecision:
		p = &Command_StoreDecision{StoreDecision: x}
	case *TxnBatch:
		p = &Command_Batch{Batch: x}
	case *MarkTxnTimedout:
		p = &Command_MarkTimedout{MarkTimedout: x}
	default:
		return nil, errors.Errorf("newCommand: unhandled payload type %T", payload)
	}

	return &Command{
		Payload: p,
	}, nil
}

func getPayload(cmd *Command) (payload, error) {
	switch x := cmd.Payload.(type) {
	case *Command_Autocommit:
		return x.Autocommit, nil
	case *Command_Prepare:
		return x.Prepare, nil
	case *Command_Commit:
		return x.Commit, nil
	case *Command_Abort:
		return x.Abort, nil
	case *Command_StoreDecision:
		return x.StoreDecision, nil
	case *Command_Batch:
		return x.Batch, nil
	case *Command_MarkTimedout:
		return x.MarkTimedout, nil
	default:
		return nil, errors.Errorf("getPayload: unhandled payload type %T", cmd.Payload)
	}
}

func (*TxnAutocommit) isPayload()    {}
func (*TxnPrepare) isPayload()       {}
func (*TxnCommit) isPayload()        {}
func (*TxnAbort) isPayload()         {}
func (*StoreTxnDecision) isPayload() {}
func (*TxnBatch) isPayload()         {}
func (*MarkTxnTimedout) isPayload()  {}
