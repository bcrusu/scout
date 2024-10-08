package storage

import (
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/storage/kv"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/hlc"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_    multiraft.FSM = (*FSM)(nil)
	logF               = logging.WithComponent("storage_fsm").NoContext()
)

type FSM struct {
	partitionID  uint32
	db           kv.DB
	txnProcessor *txnProcessor
	lock         sync.RWMutex // guards all below
	index        uint64       // last applied raft index
}

func NewFSM(partitionID uint32, db kv.DB) *FSM {
	return &FSM{
		partitionID:  partitionID,
		db:           db,
		txnProcessor: newTxnProcessor(partitionID, db),
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

	hlc.Update(timestamp)

	logF.Debugf("Applying command %T success", payload)
	return result
}

func (f *FSM) Snapshot() ([]byte, error) {
	// TODO: call db.Sync()

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
		Index:    f.index,
		Status:   status,
		Prepared: prepared,
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

	return nil
}
