package kv

import (
	"bytes"
	"math"
)

// Keys are sorted ascending by keyspace and key and descending by timestamp.
// This results in a sort order with latest version at the start of each key range.
func CompareKeys(a, b []byte) int {
	if x := int(ExtractKeyspace(a)) - int(ExtractKeyspace(b)); x != 0 {
		return x
	} else if x = bytes.Compare(ExtractKey(a), ExtractKey(b)); x != 0 {
		return x
	} else {
		ats := ExtractTimestamp(a)
		bts := ExtractTimestamp(b)

		switch {
		case ats > bts:
			return -1
		case ats < bts:
			return 1
		default:
			return 0
		}
	}
}

func FirstAddress() Address {
	return Address{
		Keyspace:  0,
		Key:       []byte{},
		Timestamp: math.MaxUint64,
	}
}
