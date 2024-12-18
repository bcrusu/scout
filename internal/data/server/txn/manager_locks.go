package txn

import (
	"math"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/hlc"
)

// checkLocks implementation is not that clever as it simply iterates the input locks to compare
// each with all currently held locks; the quadratic runtime complexity can be avoided by using
// an interval tree data structure with logarithmic runtime.
// https://en.wikipedia.org/wiki/Interval_tree
func (p *Manager) checkLocks(id id, timestamp uint64, locks []*data.Lock) *data.TxnStatus {
	for _, lock := range locks {
		if !p.acquireLock(lock) {
			p.meters.LocksFailed.Add(1)
			return newFailedStatus(id, timestamp, lock.ActionId, data.ActionStatus_LockFailed)
		}
	}

	p.meters.LocksHeld.Add(len(locks))
	return nil
}

func (p *Manager) acquireLock(lock *data.Lock) bool {
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

// Essentially, the latest read timestamp is computed in a similar fashion
// as the Spanner 'safe time' timestamp, and taken as the min of the timestamp
// of the highest applied write txn and the min of all conflicting prepared
// txn timestamps minus 1.
// More details in the paper "Spanner: Google’s Globally-Distributed Database"
// in section 4.1.3 "Serving Reads at a Timestamp".
func (p *Manager) latestReadTimestampForLocks(locks []*data.Lock) uint64 {
	conflicting := uint64(math.MaxUint64)

	for _, lock := range locks {
		conflicting = min(conflicting, p.latestReadTimestampForLock(lock))
	}

	maxTimestamp := p.maxTimestamp

	// if the store is empty:
	if maxTimestamp == 0 {
		maxTimestamp = hlc.Now()
	}

	return min(maxTimestamp, conflicting-1)
}

func (p *Manager) latestReadTimestampForLock(lock *data.Lock) uint64 {
	result := uint64(math.MaxUint64)

	for id, prepared := range p.prepared {
		if prepared.LocksReleased {
			continue
		}

		for _, other := range prepared.Locks {
			if !lock.CompatibleWith(other) {
				status := p.status[id]
				result = min(result, status.Timestamp)
				break
			}
		}
	}

	return result
}
