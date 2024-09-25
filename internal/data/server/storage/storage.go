package storage

func (t *Txn) buildLocks() []*Lock {
	locks := make([]*Lock, 0, len(t.Actions))
	for _, a := range t.Actions {
		if lock := a.buildLock(); lock != nil {
			locks = append(locks, lock)
		}
	}
	return locks
}

func (a *Action) buildLock() *Lock {
	switch x := a.Payload.(type) {
	case *Action_Insert:
		return &Lock{Payload: &Lock_Key{&KeyLock{
			Keyspace:  x.Insert.Keyspace,
			Key:       x.Insert.Key,
			Exclusive: true,
		}}}
	case *Action_Update:
		return &Lock{Payload: &Lock_Key{&KeyLock{
			Keyspace:  x.Update.Keyspace,
			Key:       x.Update.Key,
			Exclusive: true,
		}}}
	case *Action_Delete:
		return &Lock{Payload: &Lock_Key{&KeyLock{
			Keyspace:  x.Delete.Keyspace,
			Key:       x.Delete.Key,
			Exclusive: true,
		}}}
	case *Action_Upsert:
		return &Lock{Payload: &Lock_Key{&KeyLock{
			Keyspace:  x.Upsert.Keyspace,
			Key:       x.Upsert.Key,
			Exclusive: true,
		}}}
	case *Action_LockKey:
		return &Lock{Payload: &Lock_Key{x.LockKey.Lock}}
	case *Action_LockRange:
		return &Lock{Payload: &Lock_Range{x.LockRange.Lock}}
	default:
		return nil
	}
}
