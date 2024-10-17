package kv

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"slices"
)

type Address struct {
	Keyspace  uint32
	Key       []byte
	Timestamp uint64
}

func NewAddress(kyespace uint32, key []byte, timestamp uint64) Address {
	return Address{
		Keyspace:  kyespace,
		Key:       key,
		Timestamp: timestamp,
	}
}

func FirstAddress(keyspace uint32) Address {
	return NewAddress(keyspace, []byte{}, math.MaxUint64)
}

func FirstAddressForKey(keyspace uint32, key []byte) Address {
	return NewAddress(keyspace, key, math.MaxUint64)
}

func LastAddressForKey(keyspace uint32, key []byte) Address {
	return NewAddress(keyspace, key, 0)
}

func (a Address) Next() Address {
	timestamp := a.Timestamp - 1
	if timestamp != math.MaxUint64 {
		return NewAddress(a.Keyspace, a.Key, timestamp)
	}

	key := slices.Clone(a.Key)
	for i := range key {
		key[i] = key[i] + 1
		if key[i] != 0 {
			return NewAddress(a.Keyspace, key, timestamp)
		}
	}

	return NewAddress(a.Keyspace+1, key, timestamp)
}

func (a Address) NextKey() Address {
	key := slices.Clone(a.Key)
	for i := range key {
		key[i] = key[i] + 1
		if key[i] != 0 {
			return NewAddress(a.Keyspace, key, math.MaxUint64)
		}
	}

	return NewAddress(a.Keyspace+1, key, math.MaxUint64)
}

// Keys are sorted ascending by keyspace and key and descending by timestamp.
// This results in a sort order with latest version at the start of each key range.
func (a Address) Compare(b Address) int {
	if x := int(a.Keyspace) - int(b.Keyspace); x != 0 {
		return x
	} else if x = bytes.Compare(a.Key, b.Key); x != 0 {
		return x
	} else {
		switch {
		case a.Timestamp > b.Timestamp:
			return -1
		case a.Timestamp < b.Timestamp:
			return 1
		default:
			return 0
		}
	}
}

func (a Address) Before(other Address) bool {
	return a.Compare(other) < 0
}

func (a Address) After(other Address) bool {
	return a.Compare(other) > 0
}

func (a Address) String() string {
	return fmt.Sprintf("keyspace=%d key=%s timestamp=%d",
		a.Keyspace, base64.RawURLEncoding.EncodeToString(a.Key), a.Timestamp)
}
