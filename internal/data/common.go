package data

import (
	"fmt"

	"github.com/bcrusu/scout/internal/errors"
)

func (t *Txn) IsReplicaRead() bool {
	for _, a := range t.Actions {
		if !a.IsReplicaRead() {
			return false
		}
	}
	return true
}

func (s TxnState) IsFinal() bool {
	switch s {
	case TxnState_Pending, TxnState_Prepared, TxnState_Decided:
		return false
	case TxnState_Committed, TxnState_Aborted, TxnState_Failed, TxnState_Timedout:
		return true
	default:
		panic(fmt.Sprintf("unhandled txn state %s", s))
	}
}

func (t *Action) IsReplicaRead() bool {
	switch x := t.Payload.(type) {
	case *Action_Read:
		// replicas can only handle snapshot/history reads where
		// the timestamp is specified. All other reads must go
		// through leader to ensure consistency.
		if x.Read.Timestamp == 0 {
			return false
		}
	default:
		return false
	}

	return true
}

func (r *ActionStatus) ToResult() (*Value, error) {
	return r.Value, r.Code.ToError()
}

func (r *ActionStatus) ToError() error {
	return r.Code.ToError()
}

func (c ActionStatus_Code) ToError() error {
	switch c {
	case ActionStatus_Success:
		return nil
	case ActionStatus_KeyNotFound:
		return errors.NotFound
	case ActionStatus_KeyAlreadyExists:
		return errors.AlreadyExists
	case ActionStatus_LockCheckFailed:
		return errors.FailedPrecondition
	case ActionStatus_LockFailed:
		return errors.TransactionAborted
	case ActionStatus_CorruptedData:
		return errors.CorruptedData
	default:
		return errors.Errorf("unhandled action code %s", c)
	}
}
