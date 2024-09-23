package storage

import (
	"bytes"
	"slices"
)

func (f *FSM) applySet(cmd *Set) (*SetResult, error) {
	keyspace, ok := f.keyspaces[cmd.Keyspace]
	if !ok {
		keyspace = &Keyspace{}
		f.keyspaces[cmd.Keyspace] = keyspace
	}

	i, found := slices.BinarySearchFunc(keyspace.Items, cmd.Key, func(kv *Keyspace_KV, key []byte) int {
		return bytes.Compare(kv.Key, key)
	})

	if found {
		keyspace.Items[i].Value = cmd.Value
	} else {
		kv := &Keyspace_KV{Key: cmd.Key, Value: cmd.Value}
		keyspace.Items = append(keyspace.Items, kv)

		slices.SortFunc(keyspace.Items, func(a, b *Keyspace_KV) int {
			return bytes.Compare(a.Key, b.Key)
		})
	}

	return &SetResult{
		Updated: found,
	}, nil
}
