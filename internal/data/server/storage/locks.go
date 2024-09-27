package storage

import (
	"github.com/bcrusu/graph/internal/data"
)

// checkLocks implementation is not that clever as it simply iterates the input locks to compare
// each with all currently held locks; the quadratic runtime complexity can be avoided by using
// an interval tree data structure with logarithmic runtime.
// https://en.wikipedia.org/wiki/Interval_tree
func (p *txnProcessor) checkLocks(id TxnId, timestamp uint64, locks []*Lock) *data.TxnStatus {
	for _, lock := range locks {
		if !p.checkLock(lock) {
			return newActionFailedStatus(id, timestamp, int(lock.ActionIndex), data.ErrorCode_LockFailed)
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
	for i, a := range t.Actions {
		if lock := buildActionLock(i, a); lock != nil {
			locks = append(locks, lock)
		}
	}
	return locks
}

func buildActionLock(i int, a *data.Action) *Lock {
	switch x := a.Payload.(type) {
	case *data.Action_Insert:
		return &Lock{
			ActionIndex: uint32(i),
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Insert.Keyspace,
				Key:       x.Insert.Key,
				Exclusive: true,
			}}}
	case *data.Action_Update:
		return &Lock{
			ActionIndex: uint32(i),
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Update.Keyspace,
				Key:       x.Update.Key,
				Exclusive: true,
			}}}
	case *data.Action_Delete:
		return &Lock{
			ActionIndex: uint32(i),
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Delete.Keyspace,
				Key:       x.Delete.Key,
				Exclusive: true,
			}}}
	case *data.Action_Upsert:
		return &Lock{
			ActionIndex: uint32(i),
			Payload: &Lock_KeyLock{&data.KeyLock{
				Keyspace:  x.Upsert.Keyspace,
				Key:       x.Upsert.Key,
				Exclusive: true,
			}}}
	case *data.Action_LockKey:
		return &Lock{
			ActionIndex: uint32(i),
			Payload:     &Lock_KeyLock{x.LockKey.Lock},
		}
	case *data.Action_LockRange:
		return &Lock{
			ActionIndex: uint32(i),
			Payload:     &Lock_RangeLock{x.LockRange.Lock},
		}
	default:
		return nil
	}
}
