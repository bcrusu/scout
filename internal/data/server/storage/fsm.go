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
	_    multiraft.FSM = (*FSM)(nil)
	logF               = logging.WithComponent("storage_fsm").NoContext()
)

type FSM struct {
	lock      sync.RWMutex // guards all below
	index     uint64       // last applied raft index
	version   uint64       // used for optimistic concurrency control
	keyspaces map[uint64]*Keyspace
}

func NewFSM() *FSM {
	return &FSM{
		keyspaces: map[uint64]*Keyspace{},
	}
}

func (f *FSM) Apply(index uint64, appendedAt time.Time, data []byte) any {
	log := logF.With("index", index, "appendedAt", appendedAt)

	f.lock.Lock()
	defer f.lock.Unlock()

	var result any

	if cmd, err := utils.UnmarshalProto[Command](data); err != nil {
		log.WithError(err).Debug("UnmarshalProto failed")
		result = err
	} else {
		result = f.applyCommand(cmd, log)
	}

	f.index = index
	return result
}

func (f *FSM) applyCommand(cmd *Command, log logging.LoggerNoContext) any {
	if cmd.IfMatch != 0 && cmd.IfMatch != f.version {
		log.Debug("Command version check failed", "fsm_version", f.version, "cmd_version", cmd.IfMatch)
		return errors.FailedPrecondition
	}

	payload, err := getPayload(cmd)
	if err != nil {
		log.WithError(err).Debug("getPayload failed")
		return err
	}

	var result any

	log.Debugf("Applying command %T...", payload)

	switch x := payload.(type) {
	case *Set:
		result, err = f.applySet(x)
	case *Delete:
		result, err = f.applyDelete(x)
	default:
		return errors.Errorf("apply: unhandled payload type %T", payload)
	}

	if err != nil {
		log.WithError(err).Debugf("Applying command %T failed", payload)
		return err
	}

	f.version++
	logF.Debugf("Applying command %T success", payload)

	return result
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	snap := &Snapshot{
		Index:     f.index,
		Version:   f.version,
		Keyspaces: f.keyspaces,
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
	f.version = snap.Version
	f.keyspaces = snap.Keyspaces
	return nil
}
