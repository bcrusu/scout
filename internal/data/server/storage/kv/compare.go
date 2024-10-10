package kv

import (
	"bytes"
)

// Keys are sorted ascending by keyspace and key and descending by timestamp.
// This results in a sort order with latest version at the start of each key range.
func CompareKeys(a, b []byte) int {
	addr1 := DecodeAddress(a)
	addr2 := DecodeAddress(b)

	if x := int(addr1.Keyspace) - int(addr2.Keyspace); x != 0 {
		return x
	} else if x = bytes.Compare(addr1.Key, addr2.Key); x != 0 {
		return x
	} else {
		switch {
		case addr1.Timestamp > addr2.Timestamp:
			return -1
		case addr1.Timestamp < addr2.Timestamp:
			return 1
		default:
			return 0
		}
	}
}
