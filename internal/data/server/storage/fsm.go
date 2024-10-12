package storage

import (
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
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
	partitionID  uint32
	db           kv.DB
	txnProcessor *txnProcessor
	log          logging.LoggerNoContext
	lock         sync.RWMutex // guards all below
	index        uint64       // last applied raft index
	maxTimestamp uint64       // max HCL timestamp
}

func NewFSM(partitionID uint32, db kv.DB) *FSM {
	return &FSM{
		partitionID:  partitionID,
		db:           db,
		txnProcessor: newTxnProcessor(partitionID, db),
		log:          logging.WithComponent("storage_fsm").With("parttition", partitionID).NoContext(),
	}
}

func (f *FSM) Apply(index uint64, appendedAt time.Time, data []byte) any {
	log := f.log.With("index", index, "appendedAt", appendedAt)

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

func (f *FSM) applyCommand(index uint64, _ time.Time, cmd *Command, log logging.LoggerNoContext) any {
	payload := getPayload(cmd)
	var result any
	var timestamp uint64

	log.Debugf("Applying command %T...", payload)

	switch x := payload.(type) {
	case *TxnBatch:
		timestamp = x.MaxTimestamp()
		result = f.txnProcessor.applyBatch(index, x)
	default:
		return errors.Errorf("apply: unhandled payload type %T", payload)
	}

	f.maxTimestamp = max(f.maxTimestamp, timestamp)

	log.Debugf("Applying command %T success", payload)
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

	status := make([]*data.TxnStatus, 0, len(f.txnProcessor.status))
	for _, s := range f.txnProcessor.status {
		status = append(status, s)
	}

	prepared := make([]*data.Txn, 0, len(f.txnProcessor.prepared))
	for _, p := range f.txnProcessor.prepared {
		prepared = append(prepared, p.Txn)
	}

	snap := &Snapshot{
		Index:        f.index,
		Status:       status,
		Prepared:     prepared,
		MaxTimestamp: f.maxTimestamp,
	}

	data, err := utils.MarshalProto(snap)
	return data, err
}

func (f *FSM) Restore(snapshot []byte) error {
	snap, err := utils.UnmarshalProto[Snapshot](snapshot)
	if err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	status := make(map[TxnId]*data.TxnStatus, len(snap.Status))
	for _, s := range snap.Status {
		status[NewTxnId(s.Id)] = s
	}

	prepared := make(map[TxnId]*txnLocks, len(snap.Prepared))
	for _, txn := range snap.Prepared {
		prepared[NewTxnId(txn.Id)] = &txnLocks{
			Txn:   txn,
			Locks: buildLocks(txn),
		}
	}

	f.index = snap.Index
	f.txnProcessor.status = status
	f.txnProcessor.prepared = prepared
	f.maxTimestamp = snap.MaxTimestamp

	return nil
}
