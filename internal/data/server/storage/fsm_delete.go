package storage

import (
	"bytes"
	"slices"
)

func (f *FSM) applyDelete(cmd *Delete) (*DeleteResult, error) {
	keyspace, ok := f.keyspaces[cmd.Keyspace]
	if !ok {
		return &DeleteResult{Deleted: false}, nil
	}

	i, found := slices.BinarySearchFunc(keyspace.Items, cmd.Key, func(kv *Keyspace_KV, key []byte) int {
		return bytes.Compare(kv.Key, key)
	})

	if found {
		keyspace.Items = append(keyspace.Items[:i], keyspace.Items[i+1:]...)
	}

	return &DeleteResult{
		Deleted: found,
	}, nil
}
