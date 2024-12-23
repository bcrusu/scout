package txn

import (
	"math"

	"github.com/bcrusu/scout/internal/data"
)

// checkLocks implementation is not that clever as it simply iterates the input locks to compare
// each with all currently held locks; the quadratic runtime complexity can be avoided by using
// an interval tree data structure with logarithmic runtime.
// https://en.wikipedia.org/wiki/Interval_tree
func (p *Manager) checkLocks(id id, timestamp uint64, locks []*data.Lock) *data.TxnStatus {
	for _, lock := range locks {
		if !p.checkLock(lock) {
			p.meters.LocksFailed.Add(1)
			return newFailedStatus(id, timestamp, lock.ActionId, data.ActionStatus_LockFailed)
		}
	}

	return nil
}

func (p *Manager) checkLock(lock *data.Lock) bool {
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

func (p *Manager) releaseLocks(prepared *data.Prepared) {
	p.meters.LocksHeld.Add(-len(prepared.Locks))
	prepared.Locks = nil
	prepared.LocksReleased = true
}

func (p *Manager) countLocksHeld() int {
	count := 0
	for _, prepared := range p.prepared {
		count += len(prepared.Locks)
	}
	return count
}

// Essentially, the 'safe' read timestamp is computed in a similar fashion
// as the Spanner 'safe time' timestamp, and taken as the min of the highest
// applied write timestamp and the min of all conflicting prepared txn
// timestamps minus 1.
// More details in the paper "Spanner: Google’s Globally-Distributed Database"
// in Section 4.1.3 "Serving Reads at a Timestamp" for calculation formula and
// in Section 4.2.4 "Refinements" for false conflict handling.
func (p *Manager) safeTimestampForLocks(locks []*data.Lock) (uint64, bool) {
	conflicting := uint64(math.MaxUint64)

	for _, lock := range locks {
		conflicting = min(conflicting, p.safeTimestampForLock(lock))
	}

	if conflicting == math.MaxUint64 {
		return p.maxTimestamp, false
	}

	return min(p.maxTimestamp, conflicting-1), true
}

func (p *Manager) safeTimestampForLock(lock *data.Lock) uint64 {
	result := uint64(math.MaxUint64)

	for id, prepared := range p.prepared {
		if prepared.LocksReleased {
			continue
		}

		status := p.status[id]
		if status.State.IsFinal() {
			continue
		}

		for _, other := range prepared.Locks {
			if !lock.CompatibleWith(other) {
				result = min(result, prepared.Timestamp)
				break
			}
		}
	}

	return result
}
