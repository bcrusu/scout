package txn

import "github.com/bcrusu/graph/internal/data"

func Insert(keyspace uint64, key, value []byte) *data.Action {
	return &data.Action{Payload: &data.Action_Insert{
		Insert: &data.Insert{
			Keyspace: keyspace,
			Key:      key,
			Value:    value,
		},
	}}
}

func Update(keyspace uint64, key, value []byte) *data.Action {
	return &data.Action{Payload: &data.Action_Update{
		Update: &data.Update{
			Keyspace: keyspace,
			Key:      key,
			Value:    value,
		},
	}}
}

func Upsert(keyspace uint64, key, value []byte) *data.Action {
	return &data.Action{Payload: &data.Action_Upsert{
		Upsert: &data.Upsert{
			Keyspace: keyspace,
			Key:      key,
			Value:    value,
		},
	}}
}

func Delete(keyspace uint64, key []byte) *data.Action {
	return &data.Action{Payload: &data.Action_Delete{
		Delete: &data.Delete{
			Keyspace: keyspace,
			Key:      key,
		},
	}}
}

func LockKey(keyspace uint64, key []byte, exclusive bool, check data.LockKey_Check) *data.Action {
	return &data.Action{Payload: &data.Action_LockKey{
		LockKey: &data.LockKey{
			Check: check,
			Lock: &data.KeyLock{
				Keyspace:  keyspace,
				Key:       key,
				Exclusive: exclusive,
			},
		},
	}}
}

func LockRange(keyspace uint64, startKey, endKey []byte, exclusive bool, check data.LockRange_Check) *data.Action {
	return &data.Action{Payload: &data.Action_LockRange{
		LockRange: &data.LockRange{
			Check: check,
			Lock: &data.RangeLock{
				Keyspace:  keyspace,
				StartKey:  startKey,
				EndKey:    endKey,
				Exclusive: exclusive,
			},
		},
	}}
}
