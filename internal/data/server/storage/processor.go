package storage

import (
	"github.com/bcrusu/graph/internal/data"
)

// TODO: prune status for old txn
type txnProcessor struct {
	partitionID uint32
	db          *crticalDB
	status      map[TxnId]*data.TxnStatus
	prepared    map[TxnId]*TxnPrepared
}

func newTxnProcessor(partitionID uint32, db DB) *txnProcessor {
	return &txnProcessor{
		partitionID: partitionID,
		db:          &crticalDB{db},
		status:      map[TxnId]*data.TxnStatus{},
		prepared:    map[TxnId]*TxnPrepared{},
	}
}

// TODO: parallel execution
func (p *txnProcessor) applyBatch(batch *ExecuteTxnBatch) (*TxnBatchResult, error) {
	status := make([]*data.TxnStatus, 0, batch.totalLen())

	for _, txn := range batch.Autocommit {
		id := newTxnId(txn.Id)
		status = append(status, p.autocommit(id, batch.Timestamp, txn))
	}

	for _, txn := range batch.TwoPhasePrepare {
		id := newTxnId(txn.Id)
		status = append(status, p.prepare(id, batch.Timestamp, txn))
	}

	for _, tid := range batch.TwoPhaseCommit {
		id := newTxnId(tid)
		status = append(status, p.commit(id, batch.Timestamp))
	}

	for _, tid := range batch.TwoPhaseAbort {
		id := newTxnId(tid)
		status = append(status, p.abort(id, batch.Timestamp))
	}

	return &TxnBatchResult{status}, nil
}

func (p *txnProcessor) autocommit(id TxnId, timestamp uint64, txn *data.Txn) *data.TxnStatus {
	if status, ok := p.status[id]; ok {
		return status
	}

	locks := buildLocks(txn)
	status := p.checkLocks(id, timestamp, locks)

	if status == nil {
		status = p.validate(id, timestamp, txn)

		if status == nil {
			p.write(timestamp, txn)
		}
	}

	if status == nil {
		status = newTxnStatus(id, timestamp, data.TxnState_Committed)
	}

	p.status[id] = status
	return status
}

func (p *txnProcessor) prepare(id TxnId, timestamp uint64, txn *data.Txn) *data.TxnStatus {
	if status, ok := p.status[id]; ok {
		return status
	}

	locks := buildLocks(txn)
	status := p.checkLocks(id, timestamp, locks)

	if status == nil {
		status = p.validate(id, timestamp, txn)
	}

	if status == nil {
		p.prepared[id] = &TxnPrepared{
			Txn:   txn,
			Locks: locks,
		}
		status = newTxnStatus(id, timestamp, data.TxnState_Prepared)
	}

	p.status[id] = status
	return status
}

func (p *txnProcessor) commit(id TxnId, timestamp uint64) *data.TxnStatus {
	if status, ok := p.status[id]; ok && status.State == data.TxnState_Committed {
		return status
	}

	prepared, ok := p.prepared[id]
	if !ok {
		return newTxnStatus(id, timestamp, data.TxnState_NotFound)
	}

	p.write(timestamp, prepared.Txn)
	status := newTxnStatus(id, timestamp, data.TxnState_Committed)

	delete(p.prepared, id)
	p.status[id] = status
	return status
}

func (p *txnProcessor) abort(id TxnId, timestamp uint64) *data.TxnStatus {
	if status, ok := p.status[id]; ok && status.State == data.TxnState_Aborted {
		return status
	}

	if _, ok := p.prepared[id]; !ok {
		return newTxnStatus(id, timestamp, data.TxnState_NotFound)
	}

	status := newTxnStatus(id, timestamp, data.TxnState_Aborted)

	delete(p.prepared, id)
	p.status[id] = status
	return status
}

func (p *txnProcessor) validate(id TxnId, timestamp uint64, txn *data.Txn) *data.TxnStatus {
	for i, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Insert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Insert.Keyspace,
				Key:         x.Insert.Key,
			}

			// TODO: validation code is not idempotent!
			if p.db.Exists(loc) {
				return newActionFailedStatus(id, timestamp, i, data.ErrorCode_KeyAlreadyExists)
			}
		case *data.Action_Update:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Update.Keyspace,
				Key:         x.Update.Key,
			}

			if !p.db.Exists(loc) {
				return newActionFailedStatus(id, timestamp, i, data.ErrorCode_KeyNotFound)
			}
		case *data.Action_Upsert:
			// pass
		case *data.Action_Delete:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Delete.Keyspace,
				Key:         x.Delete.Key,
			}

			if !p.db.Exists(loc) {
				return newActionFailedStatus(id, timestamp, i, data.ErrorCode_KeyNotFound)
			}
		case *data.Action_LockKey:
			if x.LockKey.Check == data.LockKey_None {
				continue
			}

			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.LockKey.Lock.Keyspace,
				Key:         x.LockKey.Lock.Key,
			}

			actual := p.db.Exists(loc)
			expected := x.LockKey.Check == data.LockKey_MustExist

			if actual != expected {
				return newActionFailedStatus(id, timestamp, i, data.ErrorCode_LockCheckFailed)
			}
		case *data.Action_LockRange:
			if x.LockRange.Check == data.LockRange_None {
				continue
			}

			rang := Range{
				PartitionID: p.partitionID,
				Keyspace:    x.LockRange.Lock.Keyspace,
				StartKey:    x.LockRange.Lock.StartKey,
				EndKey:      x.LockRange.Lock.EndKey,
			}

			actual := p.db.ExistsInRange(rang)
			expected := x.LockRange.Check == data.LockRange_MustNotBeEmpty

			if actual != expected {
				return newActionFailedStatus(id, timestamp, i, data.ErrorCode_LockCheckFailed)
			}
		}
	}

	return nil
}

func (p *txnProcessor) write(timestamp uint64, txn *data.Txn) {
	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Insert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Insert.Keyspace,
				Key:         x.Insert.Key,
				Timestamp:   timestamp,
			}

			p.db.Set(loc, x.Insert.Value)
		case *data.Action_Update:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Update.Keyspace,
				Key:         x.Update.Key,
				Timestamp:   timestamp,
			}

			p.db.Set(loc, x.Update.Value)
		case *data.Action_Upsert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Upsert.Keyspace,
				Key:         x.Upsert.Key,
				Timestamp:   timestamp,
			}

			p.db.Set(loc, x.Upsert.Value)
		case *data.Action_Delete:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Delete.Keyspace,
				Key:         x.Delete.Key,
				Timestamp:   timestamp,
			}

			p.db.Delete(loc)
		}
	}
}

func newTxnStatus(id TxnId, timestamp uint64, state data.TxnState) *data.TxnStatus {
	return &data.TxnStatus{
		Id:        id.Proto(),
		Timestamp: timestamp,
		State:     state,
	}
}

func newActionFailedStatus(id TxnId, timestamp uint64, index int, code data.ErrorCode) *data.TxnStatus {
	return &data.TxnStatus{
		Id:          id.Proto(),
		Timestamp:   timestamp,
		State:       data.TxnState_ActionFailed,
		ActionIndex: uint32(index),
		ActionError: code,
	}
}
