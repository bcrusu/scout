package storage

import (
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/data"
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
	partitionID  uint32
	txnProcessor *txnProcessor
	lock         sync.RWMutex // guards all below
	index        uint64       // last applied raft index
}

func NewFSM(partitionID uint32, db DB) *FSM {
	return &FSM{
		txnProcessor: newTxnProcessor(partitionID, db),
		partitionID:  partitionID,
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
		result = f.applyCommand(appendedAt, cmd, log)
	}

	f.index = index
	return result
}

func (f *FSM) applyCommand(_ time.Time, cmd *Command, log logging.LoggerNoContext) any {
	payload, err := getPayload(cmd)
	if err != nil {
		log.WithError(err).Debug("getPayload failed")
		return err
	}

	var result any

	log.Debugf("Applying command %T...", payload)

	switch x := payload.(type) {
	case *TxnAutocommit:
		result, err = f.txnProcessor.applyAutocommit(x)
	case *TxnPrepare:
		result, err = f.txnProcessor.applyPrepare(x)
	case *TxnCommit:
		result, err = f.txnProcessor.applyCommit(x)
	case *TxnAbort:
		result, err = f.txnProcessor.applyAbort(x)
	case *StoreTxnDecision:
		result, err = f.txnProcessor.applyStoreDecision(x)
	case *MarkTxnTimedout:
		result, err = f.txnProcessor.applyMarkTimedout(x)
	case *TxnBatch:
		result = f.txnProcessor.applyBatch(x)
	default:
		return errors.Errorf("apply: unhandled payload type %T", payload)
	}

	if err != nil {
		log.WithError(err).Debugf("Applying command %T failed", payload)
		return err
	}

	logF.Debugf("Applying command %T success", payload)
	return result
}

func (f *FSM) Snapshot() ([]byte, error) {
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
