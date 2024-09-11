package storage

import (
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   multiraft.FSM = (*FSM)(nil)
	log               = logging.WithComponent("data_storage").NoContext()
)

type FSM struct {
	lock  sync.RWMutex
	index uint64
	items map[string][]byte
}

func NewFSM() *FSM {
	return &FSM{
		items: map[string][]byte{},
	}
}

func (f *FSM) Apply(index uint64, appendedAt time.Time, data []byte) any {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.index = index

	cmd, err := utils.UnmarshalProto[Command](data)
	if err != nil {
		return err
	}

	payload, err := getPayload(cmd)
	if err != nil {
		return err
	}

	log := log.With("index", index, "appendedAt", appendedAt)

	switch x := payload.(type) {
	case *Set:
		key := string(x.Key)

		oldValue := f.items[key]
		f.items[key] = x.Value
		log.Debug("FSM.Set", "key", x.Key, "old_value", oldValue, "new_value", x.Value)
	case *Delete:
		key := string(x.Key)

		oldValue, found := f.items[key]
		if found {
			delete(f.items, key)
			log.Debug("FSM.Deleted", "key", x.Key, "old_value", oldValue)
		} else {
			log.Debug("FSM.Delete key not found", "key", x.Key)
		}
	default:
		return errors.Errorf("FSM.Apply: unhandled payload type %T", payload)
	}

	return nil
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	snap := &Snapshot{
		Index: f.index,
		Items: f.items,
	}

	data, err := utils.MarshalProto(snap)
	return data, err
}

func (f *FSM) Restore(data []byte) error {
	snap, err := utils.UnmarshalProto[Snapshot](data)
	if err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.index = snap.Index
	f.items = snap.Items
	return nil
}
