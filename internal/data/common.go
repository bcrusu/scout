package data

import (
	"fmt"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
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

func (a *Action) IsReplicaRead() bool {
	switch x := a.Payload.(type) {
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

func (t *TxnId) Validate() error {
	if t == nil {
		return errors.Error("TxnId is nil")
	}
	if t.ServerId == 0 || t.Timestamp == 0 {
		return errors.Error("TxnId has missing fields")
	}
	return nil
}

func (t *Txn) Validate() error {
	if t == nil {
		return errors.Error("Txn is nil")
	}
	if err := t.Id.Validate(); err != nil {
		return errors.Wrap(err, "Txn.Id is invalid")
	}
	if len(t.Actions) == 0 || len(t.ParticipantPids) == 0 {
		return errors.Error("Txn has missing fields")
	}

	for _, action := range t.Actions {
		if err := action.Validate(); err != nil {
			return errors.Wrap(err, "Txn.Actions is invalid")
		}
	}
	return nil
}

func (a *Action) Validate() error {
	if a == nil {
		return errors.Error("Action is nil")
	}

	switch p := a.Payload.(type) {
	case *Action_Read:
		if p.Read == nil {
			return errors.Error("Action.Read is nil")
		}
		if p.Read.Keyspace < keyspace.FirstUserKeyspace || len(p.Read.Key) == 0 {
			return errors.Error("Action.Read has invalid fields")
		}
	case *Action_Insert:
		if p.Insert == nil {
			return errors.Error("Action.Insert is nil")
		}
		if p.Insert.Keyspace < keyspace.FirstUserKeyspace || len(p.Insert.Key) == 0 {
			return errors.Error("Action.Insert has invalid fields")
		}
	case *Action_Update:
		if p.Update == nil {
			return errors.Error("Action.Update is nil")
		}
		if p.Update.Keyspace < keyspace.FirstUserKeyspace || len(p.Update.Key) == 0 {
			return errors.Error("Action.Update has invalid fields")
		}
	case *Action_Upsert:
		if p.Upsert == nil {
			return errors.Error("Action.Upsert is nil")
		}
		if p.Upsert.Keyspace < keyspace.FirstUserKeyspace || len(p.Upsert.Key) == 0 {
			return errors.Error("Action.Upsert has invalid fields")
		}
	case *Action_Delete:
		if p.Delete == nil {
			return errors.Error("Action.Delete is nil")
		}
		if p.Delete.Keyspace < keyspace.FirstUserKeyspace || len(p.Delete.Key) == 0 {
			return errors.Error("Action.Delete has invalid fields")
		}
	case *Action_LockKey:
		if p.LockKey == nil {
			return errors.Error("Action.LockKey is nil")
		}
		if _, ok := LockKey_Check_name[int32(p.LockKey.Check)]; !ok {
			return errors.Error("Action.LockKey.Check is invalid")
		}
		if err := p.LockKey.Lock.Validate(); err != nil {
			return errors.Wrap(err, "Action.LockKey.Lock is invalid")
		}
	case *Action_LockRange:
		if p.LockRange == nil {
			return errors.Error("Action.LockRange is nil")
		}
		if _, ok := LockRange_Check_name[int32(p.LockRange.Check)]; !ok {
			return errors.Error("Action.LockRange.Check is invalid")
		}
		if err := p.LockRange.Lock.Validate(); err != nil {
			return errors.Wrap(err, "Action.LockRange.Lock is invalid")
		}
	default:
		return errors.Error("Action.Payload is unknown.")
	}

	return nil
}

func (l *KeyLock) Validate() error {
	if l == nil {
		return errors.Error("KeyLock is nil")
	}
	if l.Keyspace < keyspace.FirstUserKeyspace || len(l.Key) == 0 {
		return errors.Error("KeyLock has invalid fields")
	}
	return nil
}

func (l *RangeLock) Validate() error {
	if l == nil {
		return errors.Error("RangeLock is nil")
	}
	if l.Keyspace < keyspace.FirstUserKeyspace || len(l.StartKey) == 0 || len(l.EndKey) == 0 {
		return errors.Error("RangeLock has invalid fields")
	}
	return nil
}

func (t *TxnStatus) Validate() error {
	if t == nil {
		return errors.Error("TxnStatus is nil")
	}
	if err := t.Id.Validate(); err != nil {
		return errors.Wrap(err, "Txn.Id is invalid")
	}
	if _, ok := TxnState_name[int32(t.State)]; !ok {
		return errors.Error("TxnStatus.State is invalid")
	}
	if t.Timestamp == 0 || len(t.ParticipantPids) == 0 {
		return errors.Error("TxnStatus has missing fields")
	}

	for id, action := range t.ActionStatus {
		if err := action.Validate(); err != nil {
			return errors.Wrap(err, "TxnStatus.ActionStatus is invalid")
		}
		if id != action.Id {
			return errors.Error("TxnStatus.ActionStatus.Id does not match")
		}
	}
	return nil
}

func (a *ActionStatus) Validate() error {
	if a == nil {
		return errors.Error("ActionStatus is nil")
	}
	if _, ok := ActionStatus_Code_name[int32(a.Code)]; !ok {
		return errors.Error("ActionStatus.Code is invalid")
	}
	return nil
}

func (d *TxnDecision) Validate() error {
	if d == nil {
		return errors.Error("TxnDecision is nil")
	}
	if err := d.Id.Validate(); err != nil {
		return errors.Wrap(err, "TxnDecision.Id is invalid")
	}
	if d.CommitTimestamp == 0 {
		return errors.Error("TxnDecision has missing fields")
	}
	return nil
}
