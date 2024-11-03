package storage

import (
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/hashicorp/raft"
)

var (
	_ multiraft.FSM = (*FSM)(nil)
)

type FSM struct {
	partitionID uint32
	db          kv.DB
	txn         *txn.Manager
	log         logging.Logger
	lock        sync.RWMutex // guards all below
	index       uint64       // last applied raft index
}

func NewFSM(partitionID uint32, db kv.DB, txn *txn.Manager) *FSM {
	return &FSM{
		partitionID: partitionID,
		db:          db,
		txn:         txn,
		log:         logging.New("fsm").With("partition", partitionID),
	}
}

func (f *FSM) Apply(index uint64, appendedAt time.Time, data []byte) any {
	log := f.log.With("index", index, "appendedAt", appendedAt)

	if f.index == 0 {
		log.Debug("Init partition.")

		if err := f.db.InitPartition(f.partitionID); err != nil {
			log.WithError(err).Error("Partition init failed.")
			utils.ShutdownNowf("Partition %d init failed.", f.partitionID)
		} else {
			log.Debug("Partition init success.")
		}
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	var result any

	if cmd, err := utils.UnmarshalProto[Command](data); err != nil {
		log.WithError(err).Debug("UnmarshalProto failed")
		result = err
	} else {
		result = f.applyCommand(index, appendedAt, cmd, log)
	}

	f.index = index
	return result
}

func (f *FSM) AppliedIndex() uint64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.index
}

func (f *FSM) applyCommand(index uint64, _ time.Time, cmd *Command, log logging.Logger) any {
	var result any

	log.Tracef("Applying command %T...", cmd.Payload)

	switch x := cmd.Payload.(type) {
	case *Command_Barrier:
		// best effort store last index
		result = f.db.Put(f.partitionID, index)
	case *Command_Batch:
		result = f.txn.ApplyBatch(index, x.Batch)
	default:
		return errors.Errorf("apply: unhandled payload type %T", cmd.Payload)
	}

	log.Debugf("Applied command %T.", cmd.Payload)
	return result
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.lock.RLock()
	expectedIndex := f.index
	f.lock.RUnlock()

	// Sync partition to disk first then read the persisted index and ensure it matches before
	// taking the snapshot. The backing key-value store is configured to run in a WAL-disabled
	// mode which relies on Raft log to provide the safety guarantees to avoid data loss.
	if err := f.db.SyncPartition(f.partitionID); err != nil {
		return nil, errors.Wrapf(err, "failed to sync partition=%d", f.partitionID)
	} else if index, err := f.db.GetIndex(f.partitionID, true); err != nil {
		return nil, errors.Wrapf(err, "failed to read persisted index=%d", f.partitionID)
	} else if index != expectedIndex {
		f.log.Warn("FSM Snapshot failed. Partition sync returned unexpected index.", "expected", expectedIndex, "actual", index)
		return nil, raft.ErrNothingNewToSnapshot // retry later
	}

	f.lock.RLock()
	defer f.lock.RUnlock()

	snap := &Snapshot{
		Index:       f.index,
		TxnSnapshot: f.txn.Snapshot(),
	}

	data, err := utils.MarshalProto(snap)

	f.log.Debug("Snapshot taken.", "index", snap.Index)
	return data, err
}

func (f *FSM) Restore(snapshot []byte) error {
	snap, err := utils.UnmarshalProto[Snapshot](snapshot)
	if err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.index = snap.Index
	f.txn.Restore(snap.TxnSnapshot)

	return nil
}
