package txn

import "github.com/bcrusu/scout/internal/data/server/txn"

func Read(keyspace uint32, key []byte) *txn.Action {
	return &txn.Action{Payload: &txn.Action_Read{
		Read: &txn.Read{
			Keyspace: keyspace,
			Key:      key,
		},
	}}
}

func ReadRange(keyspace uint32, startKey, endKey []byte, maxResults int) *txn.Action {
	return &txn.Action{Payload: &txn.Action_ReadRange{
		ReadRange: &txn.ReadRange{
			Keyspace:   keyspace,
			StartKey:   startKey,
			EndKey:     endKey,
			MaxResults: uint32(maxResults),
		},
	}}
}

func Insert(keyspace uint32, key, value []byte) *txn.Action {
	return &txn.Action{Payload: &txn.Action_Insert{
		Insert: &txn.Insert{
			Keyspace: keyspace,
			Key:      key,
			Value:    value,
		},
	}}
}

func Update(keyspace uint32, key, value []byte) *txn.Action {
	return &txn.Action{Payload: &txn.Action_Update{
		Update: &txn.Update{
			Keyspace: keyspace,
			Key:      key,
			Value:    value,
		},
	}}
}

func Upsert(keyspace uint32, key, value []byte) *txn.Action {
	return &txn.Action{Payload: &txn.Action_Upsert{
		Upsert: &txn.Upsert{
			Keyspace: keyspace,
			Key:      key,
			Value:    value,
		},
	}}
}

func Delete(keyspace uint32, key []byte) *txn.Action {
	return &txn.Action{Payload: &txn.Action_Delete{
		Delete: &txn.Delete{
			Keyspace: keyspace,
			Key:      key,
		},
	}}
}

func LockKey(keyspace uint32, key []byte, exclusive bool, check txn.LockKey_Check) *txn.Action {
	return &txn.Action{Payload: &txn.Action_LockKey{
		LockKey: &txn.LockKey{
			Check: check,
			Lock: &txn.KeyLock{
				Keyspace:  keyspace,
				Key:       key,
				Exclusive: exclusive,
			},
		},
	}}
}

func LockRange(keyspace uint32, startKey, endKey []byte, exclusive bool, check txn.LockRange_Check) *txn.Action {
	return &txn.Action{Payload: &txn.Action_LockRange{
		LockRange: &txn.LockRange{
			Check: check,
			Lock: &txn.RangeLock{
				Keyspace:  keyspace,
				StartKey:  startKey,
				EndKey:    endKey,
				Exclusive: exclusive,
			},
		},
	}}
}
