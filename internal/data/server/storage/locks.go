package storage

import (
	"github.com/bcrusu/scout/internal/data"
)

// checkLocks implementation is not that clever as it simply iterates the input locks to compare
// each with all currently held locks; the quadratic runtime complexity can be avoided by using
// an interval tree data structure with logarithmic runtime.
// https://en.wikipedia.org/wiki/Interval_tree
func (p *txnProcessor) checkLocks(id TxnId, timestamp uint64, locks []*Lock) *data.TxnStatus {
	for _, lock := range locks {
		if !p.checkLock(lock) {
			return newTxnFailedStatus(id, timestamp, lock.ActionId, data.ActionStatus_LockFailed)
		}
	}

	return nil
}

func (p *txnProcessor) checkLock(lock *Lock) bool {
	for _, prepared := range p.prepared {
		for _, other := range prepared.Locks {
			if !lock.compatibleWith(other) {
				return false
			}
		}
	}
	return true
}

func buildLocks(t *data.Txn) []*Lock {
	locks := make([]*Lock, 0, len(t.Actions))
	for _, a := range t.Actions {
		if lock := buildActionLock(a); lock != nil {
			locks = append(locks, lock)
		}
	}
	return locks
}

func buildActionLock(a *data.Action) *Lock {
	switch x := a.Payload.(type) {
	case *data.Action_Read:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Read.Keyspace,
				Key:       x.Read.Key,
				Exclusive: false,
			}}}
	case *data.Action_Insert:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Insert.Keyspace,
				Key:       x.Insert.Key,
				Exclusive: true,
			}}}
	case *data.Action_Update:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Update.Keyspace,
				Key:       x.Update.Key,
				Exclusive: true,
			}}}
	case *data.Action_Delete:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Delete.Keyspace,
				Key:       x.Delete.Key,
				Exclusive: true,
			}}}
	case *data.Action_Upsert:
		return &Lock{
			ActionId: a.Id,
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Upsert.Keyspace,
				Key:       x.Upsert.Key,
				Exclusive: true,
			}}}
	case *data.Action_LockKey:
		return &Lock{
			ActionId: a.Id,
			Payload:  &Lock_KeyLock{x.LockKey.Lock},
		}
	case *data.Action_LockRange:
		return &Lock{
			ActionId: a.Id,
			Payload:  &Lock_RangeLock{x.LockRange.Lock},
		}
	default:
		return nil
	}
}
