package storage

import (
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TODO: txn timeout expiration
// TODO: prune status for old txn
type txnProcessor struct {
	partitionID uint32
	db          *crticalDB
	status      map[uint64]*TxnStatus
	prepared    map[uint64]*TxnPrepared
}

func newTxnProcessor(partitionID uint32, db DB) *txnProcessor {
	return &txnProcessor{
		partitionID: partitionID,
		db:          &crticalDB{db},
		status:      map[uint64]*TxnStatus{},
		prepared:    map[uint64]*TxnPrepared{},
	}
}

// TODO: parallel execution
func (p *txnProcessor) applyBatch(appendedAt time.Time, batch *TxnBatch) (*TxnBatchResult, error) {
	errs := map[uint64]error{}

	for _, txn := range batch.Autocommit {
		if err := p.autocommit(appendedAt, txn); err != nil {
			errs[txn.Id] = err
		}
	}

	for _, txn := range batch.TwoPhasePrepare {
		if prepared, err := p.prepare(appendedAt, txn); err != nil {
			errs[txn.Id] = err
		} else {
			p.prepared[txn.Id] = prepared
		}
	}

	for _, id := range batch.TwoPhaseCommit {
		if prepared, ok := p.prepared[id]; !ok {
			errs[id] = errors.NotFound
		} else {
			p.commit(appendedAt, prepared)
			delete(p.prepared, id)
		}
	}

	for _, id := range batch.TwoPhaseAbort {
		if prepared, ok := p.prepared[id]; !ok {
			errs[id] = errors.NotFound
		} else {
			p.abort(appendedAt, prepared)
			delete(p.prepared, id)
		}
	}

	return &TxnBatchResult{
		Errors: errs,
	}, nil
}

func (p *txnProcessor) autocommit(appendedAt time.Time, txn *Txn) error {
	if _, ok := p.status[txn.Id]; ok {
		return errors.AlreadyExists
	}

	locks := txn.buildLocks()
	err := p.acquireLocks(locks)

	if err == nil {
		err = p.validate(txn)

		if err == nil {
			p.write(txn)
		}

		p.releaseLocks(locks)
	}

	p.status[txn.Id] = &TxnStatus{
		Id:        txn.Id,
		StartTime: timestamppb.New(appendedAt),
		EndTime:   timestamppb.New(appendedAt),
	}

	if err != nil {
		p.status[txn.Id].State = TxnState_Failed
	} else {
		p.status[txn.Id].State = TxnState_Committed
	}

	return err
}

func (p *txnProcessor) prepare(appendedAt time.Time, txn *Txn) (*TxnPrepared, error) {
	if _, ok := p.status[txn.Id]; ok {
		return nil, errors.AlreadyExists
	}

	locks := txn.buildLocks()
	err := p.acquireLocks(locks)

	if err == nil {
		err = p.validate(txn)

		if err != nil {
			p.releaseLocks(locks)
		}
	}

	p.status[txn.Id] = &TxnStatus{
		Id:        txn.Id,
		StartTime: timestamppb.New(appendedAt),
	}

	if err != nil {
		p.status[txn.Id].State = TxnState_Failed
		p.status[txn.Id].EndTime = timestamppb.New(appendedAt)
		return nil, err
	}

	p.status[txn.Id].State = TxnState_Prepared
	return &TxnPrepared{
		Txn:   txn,
		Locks: locks,
	}, nil
}

func (p *txnProcessor) commit(appendedAt time.Time, txnp *TxnPrepared) {
	p.write(txnp.Txn)
	p.releaseLocks(txnp.Locks)
	p.status[txnp.Txn.Id].State = TxnState_Committed
	p.status[txnp.Txn.Id].EndTime = timestamppb.New(appendedAt)
}

func (p *txnProcessor) abort(appendedAt time.Time, txnp *TxnPrepared) {
	p.releaseLocks(txnp.Locks)
	p.status[txnp.Txn.Id].State = TxnState_Aborted
	p.status[txnp.Txn.Id].EndTime = timestamppb.New(appendedAt)
}

func (p *txnProcessor) validate(txn *Txn) error {
	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *Action_Insert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Insert.Keyspace,
				Key:         x.Insert.Key,
			}

			// TODO: validation code is not idempotent!
			if p.db.Exists(loc) {
				return errors.AlreadyExists
			}
		case *Action_Update:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Update.Keyspace,
				Key:         x.Update.Key,
			}

			if !p.db.Exists(loc) {
				return errors.NotFound
			}
		case *Action_Upsert:
			// pass
		case *Action_Delete:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Delete.Keyspace,
				Key:         x.Delete.Key,
			}

			if !p.db.Exists(loc) {
				return errors.NotFound
			}
		case *Action_LockKey:
			if x.LockKey.Check == LockKey_None {
				continue
			}

			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.LockKey.Lock.Keyspace,
				Key:         x.LockKey.Lock.Key,
			}

			if ok := p.db.Exists(loc); ok && x.LockKey.Check == LockKey_MustNotExist {
				return errors.FailedPrecondition
			} else if !ok && x.LockKey.Check == LockKey_MustExist {
				return errors.FailedPrecondition
			}
		case *Action_LockRange:
			if x.LockRange.Check == LockRange_None {
				continue
			}

			rang := Range{
				PartitionID: p.partitionID,
				Keyspace:    x.LockRange.Lock.Keyspace,
				StartKey:    x.LockRange.Lock.StartKey,
				EndKey:      x.LockRange.Lock.EndKey,
			}

			if ok := p.db.ExistsInRange(rang); ok && x.LockRange.Check == LockRange_MustBeEmpty {
				return errors.FailedPrecondition
			} else if !ok && x.LockRange.Check == LockRange_MustNotBeEmpty {
				return errors.FailedPrecondition
			}
		}
	}

	return nil
}

func (p *txnProcessor) write(txn *Txn) {
	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *Action_Insert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Insert.Keyspace,
				Key:         x.Insert.Key,
				Version:     txn.Id,
			}

			p.db.Set(loc, x.Insert.Value)
		case *Action_Update:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Update.Keyspace,
				Key:         x.Update.Key,
				Version:     txn.Id,
			}

			p.db.Set(loc, x.Update.Value)
		case *Action_Upsert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Upsert.Keyspace,
				Key:         x.Upsert.Key,
				Version:     txn.Id,
			}

			p.db.Set(loc, x.Upsert.Value)
		case *Action_Delete:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Delete.Keyspace,
				Key:         x.Delete.Key,
			}

			p.db.Delete(loc)
		}
	}
}
