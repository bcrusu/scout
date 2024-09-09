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
	_   Store         = (*FSM)(nil)
	log               = logging.WithComponent("data_storage").NoContext()
)

// Store exposes read-only operations executed directly on the FSM backing
// storage, bypassing the Raft algorithm, which are not guaranteed to return
// the latest commited data.
type Store interface {
	Get(key []byte) ([]byte, bool)
}

type FSM struct {
	lock  sync.Mutex
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
	case *CommandSet:
		key := string(x.Key)

		oldValue := f.items[key]
		f.items[key] = x.Value
		log.Debug("FSM.Set", "key", x.Key, "old_value", oldValue, "new_value", x.Value)
	case *CommandDelete:
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

	f.index = index
	return nil
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

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

func (f *FSM) Get(key []byte) ([]byte, bool) {
	f.lock.Lock()
	defer f.lock.Unlock()

	value, ok := f.items[string(key)]
	return value, ok
}
