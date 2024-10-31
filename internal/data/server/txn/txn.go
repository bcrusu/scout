package txn

import (
	"bytes"
	"fmt"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
)

func (i *Id) id() id {
	return id{
		PrincipalPid: i.PrincipalPid,
		ServerID:     i.ServerId,
		Timestamp:    i.Timestamp,
	}
}

func (b *Batch) MaxTimestamp() uint64 {
	ts := uint64(0)

	for _, a := range b.Autocommit {
		ts = max(ts, a.Timestamp)
	}
	for _, a := range b.Prepare {
		ts = max(ts, a.Timestamp)
	}
	for _, a := range b.Commit {
		ts = max(ts, a.Timestamp)
	}
	for _, a := range b.Abort {
		ts = max(ts, a.Timestamp)
	}
	for _, a := range b.StoreDecision {
		ts = max(ts, a.Timestamp)
	}
	for _, a := range b.MarkTimedout {
		ts = max(ts, a.Timestamp)
	}

	return ts
}

func (b *Batch) ActionCount() int {
	result := 0
	for _, a := range b.Autocommit {
		result += len(a.Txn.Actions)
	}
	for _, a := range b.Prepare {
		result += len(a.Txn.Actions)
	}
	return result
}

func (t *Txn) IsReadOnly() bool {
	for _, a := range t.Actions {
		if !a.IsReadOnly() {
			return false
		}
	}
	return true
}

func (t *Txn) BuildLocks() []*Lock {
	locks := make([]*Lock, 0, len(t.Actions))
	for _, a := range t.Actions {
		if lock := a.BuildLock(); lock != nil {
			locks = append(locks, lock)
		}
	}
	return locks
}

func (p *Prepared) ReleaseLocks() {
	p.Locks = nil
	p.LocksReleased = true
}

func (s Status_State) IsFinal() bool {
	switch s {
	case Status_Pending, Status_Prepared, Status_Decided:
		return false
	case Status_Committed, Status_Aborted, Status_Failed, Status_Timedout:
		return true
	default:
		panic(fmt.Sprintf("unhandled txn state %s", s))
	}
}

func (a *Action) IsReadOnly() bool {
	switch a.Payload.(type) {
	case *Action_Read, *Action_ReadRange:
		return true
	default:
		return false
	}
}

func (a *Action) BuildLock() *Lock {
	switch x := a.Payload.(type) {
	case *Action_Read:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{KeyLock: &KeyLock{
				Keyspace:  x.Read.Keyspace,
				Key:       x.Read.Key,
				Exclusive: false,
			}}}
	case *Action_ReadRange:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_RangeLock{RangeLock: &RangeLock{
				Keyspace:  x.ReadRange.Keyspace,
				StartKey:  x.ReadRange.StartKey,
				EndKey:    x.ReadRange.EndKey,
				Exclusive: false,
			}}}
	case *Action_Insert:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{KeyLock: &KeyLock{
				Keyspace:  x.Insert.Keyspace,
				Key:       x.Insert.Key,
				Exclusive: true,
			}}}
	case *Action_Update:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{KeyLock: &KeyLock{
				Keyspace:  x.Update.Keyspace,
				Key:       x.Update.Key,
				Exclusive: true,
			}}}
	case *Action_Delete:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{KeyLock: &KeyLock{
				Keyspace:  x.Delete.Keyspace,
				Key:       x.Delete.Key,
				Exclusive: true,
			}}}
	case *Action_Upsert:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{KeyLock: &KeyLock{
				Keyspace:  x.Upsert.Keyspace,
				Key:       x.Upsert.Key,
				Exclusive: true,
			}}}
	case *Action_LockKey:
		return &Lock{
			ActionId: a.Id,
			Payload:  &Lock_KeyLock{KeyLock: x.LockKey.Lock},
		}
	case *Action_LockRange:
		return &Lock{
			ActionId: a.Id,
			Payload:  &Lock_RangeLock{RangeLock: x.LockRange.Lock},
		}
	default:
		return nil
	}
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
		panic(fmt.Sprintf("unhandled action code %s", c))
	}
}

func (l *Lock) CompatibleWith(other *Lock) bool {
	switch x := l.Payload.(type) {
	case *Lock_KeyLock:
		return x.CompatibleWith(other)
	case *Lock_RangeLock:
		return x.CompatibleWith(other)
	default:
		panic(fmt.Sprintf("unhandled lock type %T", l.Payload))
	}
}

func (l *Lock_KeyLock) CompatibleWith(other *Lock) bool {
	switch x := other.Payload.(type) {
	case *Lock_KeyLock:
		return keyLocksCompatible(l.KeyLock, x.KeyLock)
	case *Lock_RangeLock:
		return rangeLockCompatibleWithKeyLock(x.RangeLock, l.KeyLock)
	default:
		panic(fmt.Sprintf("unhandled lock type %T", other.Payload))
	}
}

func (l *Lock_RangeLock) CompatibleWith(other *Lock) bool {
	switch x := other.Payload.(type) {
	case *Lock_KeyLock:
		return rangeLockCompatibleWithKeyLock(l.RangeLock, x.KeyLock)
	case *Lock_RangeLock:
		return rangeLocksCompatible(l.RangeLock, x.RangeLock)
	default:
		panic(fmt.Sprintf("unhandled lock type %T", other.Payload))
	}
}

func keyLocksCompatible(a, b *KeyLock) bool {
	if a.Keyspace != b.Keyspace {
		return true
	} else if !bytes.Equal(a.Key, b.Key) {
		return true
	} else {
		return !a.Exclusive && !b.Exclusive
	}
}

func rangeLocksCompatible(a, b *RangeLock) bool {
	if a.Keyspace != b.Keyspace {
		return true
	} else if x := bytes.Compare(a.EndKey, b.StartKey); x <= 0 {
		return true
	} else if x := bytes.Compare(a.StartKey, b.EndKey); x >= 0 {
		return true
	} else {
		return !a.Exclusive && !b.Exclusive
	}
}

func rangeLockCompatibleWithKeyLock(a *RangeLock, b *KeyLock) bool {
	if a.Keyspace != b.Keyspace {
		return true
	} else if x := bytes.Compare(b.Key, a.StartKey); x < 0 {
		return true
	} else if x := bytes.Compare(b.Key, a.EndKey); x >= 0 {
		return true
	} else {
		return !a.Exclusive && !b.Exclusive
	}
}

func (t *Id) Validate() error {
	if t == nil {
		return errors.Error("Id is nil")
	}
	if t.ServerId == 0 || t.Timestamp == 0 {
		return errors.Error("Id has missing fields")
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
	if len(t.Actions) == 0 {
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
	case *Action_ReadRange:
		if p.ReadRange == nil {
			return errors.Error("Action.ReadRange is nil")
		}
		if p.ReadRange.Keyspace < keyspace.FirstUserKeyspace || len(p.ReadRange.StartKey) == 0 || len(p.ReadRange.EndKey) == 0 {
			return errors.Error("Action.ReadRange has invalid fields")
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

func (t *Status) Validate() error {
	if t == nil {
		return errors.Error("Status is nil")
	}
	if err := t.Id.Validate(); err != nil {
		return errors.Wrap(err, "Txn.Id is invalid")
	}
	if _, ok := Status_State_name[int32(t.State)]; !ok {
		return errors.Error("Status.State is invalid")
	}
	if t.Timestamp == 0 || len(t.ParticipantPids) == 0 {
		return errors.Error("Status has missing fields")
	}

	for id, action := range t.ActionStatus {
		if err := action.Validate(); err != nil {
			return errors.Wrap(err, "Status.ActionStatus is invalid")
		}
		if id != action.Id {
			return errors.Error("Status.ActionStatus.Id does not match")
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

func (d *Decision) Validate() error {
	if d == nil {
		return errors.Error("Decision is nil")
	}
	if err := d.Id.Validate(); err != nil {
		return errors.Wrap(err, "Decision.Id is invalid")
	}
	if d.CommitTimestamp == 0 {
		return errors.Error("Decision has missing fields")
	}
	return nil
}
