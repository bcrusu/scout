package txn

// checkLocks implementation is not that clever as it simply iterates the input locks to compare
// each with all currently held locks; the quadratic runtime complexity can be avoided by using
// an interval tree data structure with logarithmic runtime.
// https://en.wikipedia.org/wiki/Interval_tree
func (p *Manager) checkLocks(id id, timestamp uint64, locks []*Lock) *Status {
	for _, lock := range locks {
		if !p.checkLock(lock) {
			return newFailedStatus(id, timestamp, lock.ActionId, ActionStatus_LockFailed)
		}
	}

	return nil
}

func (p *Manager) checkLock(lock *Lock) bool {
	for _, prepared := range p.prepared {
		if prepared.LocksReleased {
			continue
		}

		for _, other := range prepared.Locks {
			if !lock.CompatibleWith(other) {
				return false
			}
		}
	}
	return true
}
