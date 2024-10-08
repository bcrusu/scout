package storage

import (
	"bytes"
	"fmt"

	"github.com/bcrusu/scout/internal/data"
)

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

func (b *TxnBatch) MaxTimestamp() uint64 {
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

func (b *TxnBatch) ActionCount() int {
	result := 0
	for _, a := range b.Autocommit {
		result += len(a.Txn.Actions)
	}
	for _, a := range b.Prepare {
		result += len(a.Txn.Actions)
	}
	return result
}
