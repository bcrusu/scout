package storage

import (
	"bytes"
	"fmt"

	"github.com/bcrusu/graph/internal/data"
)

func (b *ExecuteTxnBatch) totalLen() int {
	return len(b.Autocommit) + len(b.TwoPhasePrepare) + len(b.TwoPhaseCommit) + len(b.TwoPhaseAbort)
}

func (l *Lock) compatibleWith(other *Lock) bool {
	switch x := l.Payload.(type) {
	case *Lock_KeyLock:
		return x.compatibleWith(other)
	case *Lock_RangeLock:
		return x.compatibleWith(other)
	default:
		panic(fmt.Sprintf("unhandled lock type %T", l.Payload))
	}
}

func (l *Lock_KeyLock) compatibleWith(other *Lock) bool {
	switch x := other.Payload.(type) {
	case *Lock_KeyLock:
		return keyLocksCompatible(l.KeyLock, x.KeyLock)
	case *Lock_RangeLock:
		return rangeLockCompatibleWithKeyLock(x.RangeLock, l.KeyLock)
	default:
		panic(fmt.Sprintf("unhandled lock type %T", other.Payload))
	}
}

func (l *Lock_RangeLock) compatibleWith(other *Lock) bool {
	switch x := other.Payload.(type) {
	case *Lock_KeyLock:
		return rangeLockCompatibleWithKeyLock(l.RangeLock, x.KeyLock)
	case *Lock_RangeLock:
		return rangeLocksCompatible(l.RangeLock, x.RangeLock)
	default:
		panic(fmt.Sprintf("unhandled lock type %T", other.Payload))
	}
}

func keyLocksCompatible(a, b *data.KeyLock) bool {
	if a.Keyspace != b.Keyspace {
		return true
	} else if !bytes.Equal(a.Key, b.Key) {
		return true
	} else {
		return !a.Exclusive && !b.Exclusive
	}
}

func rangeLocksCompatible(a, b *data.RangeLock) bool {
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

func rangeLockCompatibleWithKeyLock(a *data.RangeLock, b *data.KeyLock) bool {
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
