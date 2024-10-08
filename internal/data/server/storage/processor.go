package storage

import (
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
)

// TODO: prune status for old txn
type txnProcessor struct {
	partitionID uint32
	db          *mvcc.DBBreaker
	status      map[TxnId]*data.TxnStatus
	prepared    map[TxnId]*txnLocks
	decisions   map[TxnId]*data.TxnDecision
}

type txnLocks struct {
	Txn   *data.Txn
	Locks []*Lock
}

func newTxnProcessor(partitionID uint32, db kv.DB) *txnProcessor {
	return &txnProcessor{
		partitionID: partitionID,
		db:          mvcc.NewDBBreaker(mvcc.New(partitionID, db), config.Get().DBRetryPolicy),
		status:      map[TxnId]*data.TxnStatus{},
		prepared:    map[TxnId]*txnLocks{},
	}
}

func (p *txnProcessor) applyAutocommit(cmd *TxnAutocommit) (*data.TxnStatus, []kv.Entry, error) {
	id := NewTxnId(cmd.Txn.Id)
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case data.TxnState_Committed, data.TxnState_Failed:
			// idempotent calls
			return status, nil, nil
		default:
			return nil, nil, errors.FailedPrecondition
		}
	}

	locks := buildLocks(cmd.Txn)
	status = p.checkLocks(id, cmd.Timestamp, locks)
	if status != nil {
		return status, nil, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		return status, nil, nil
	}

	writes := p.buildWriteEntries(cmd.Timestamp, cmd.Txn)
	status = newTxnStatus(id, cmd.Timestamp, data.TxnState_Committed)

	p.status[id] = status
	return status, writes, nil
}

func (p *txnProcessor) applyPrepare(cmd *TxnPrepare) (*data.TxnStatus, error) {
	id := NewTxnId(cmd.Txn.Id)
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case data.TxnState_Prepared, data.TxnState_Failed:
			// idempotent calls
			return status, nil
		default:
			return nil, errors.FailedPrecondition
		}
	}

	locks := buildLocks(cmd.Txn)
	status = p.checkLocks(id, cmd.Timestamp, locks)
	if status != nil {
		return status, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		return status, nil
	}

	p.prepared[id] = &txnLocks{
		Txn:   cmd.Txn,
		Locks: locks,
	}

	status = newTxnStatus(id, cmd.Timestamp, data.TxnState_Prepared)

	p.status[id] = status
	return status, nil
}

func (p *txnProcessor) applyCommit(cmd *TxnCommit) (*data.TxnStatus, []kv.Entry, error) {
	id := NewTxnId(cmd.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, nil, errors.NotFound
	}

	switch status.State {
	case data.TxnState_Committed:
		// idempotent calls
		return status, nil, nil
	case data.TxnState_Prepared, data.TxnState_Decided:
		prepared, ok := p.prepared[id]
		if !ok {
			return nil, nil, errors.NotFound
		}

		writes := p.buildWriteEntries(cmd.Timestamp, prepared.Txn)
		delete(p.prepared, id)
		delete(p.decisions, id)

		status := newTxnStatus(id, cmd.Timestamp, data.TxnState_Committed)
		// stores the list of participants to be able to recompose the txn results
		// after the initial client call returns.
		status.ParticipantPids = prepared.Txn.ParticipantPids

		p.status[id] = status
		return status, writes, nil
	default:
		return nil, nil, errors.FailedPrecondition
	}
}

func (p *txnProcessor) applyAbort(cmd *TxnAbort) (*data.TxnStatus, error) {
	id := NewTxnId(cmd.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnState_Aborted:
		// idempotent calls
		return status, nil
	case data.TxnState_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case data.TxnState_Prepared:
		if _, ok := p.prepared[id]; !ok {
			return nil, errors.NotFound
		}

		delete(p.prepared, id)
		delete(p.decisions, id) // presumed abort decisions are not stored, but delete just in case

		status := newTxnStatus(id, cmd.Timestamp, data.TxnState_Aborted)

		p.status[id] = status
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *txnProcessor) applyStoreDecision(cmd *StoreTxnDecision) (*data.TxnStatus, error) {
	id := NewTxnId(cmd.Decision.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, errors.NotFound
	} else if id.PrincipalPid != p.partitionID {
		return nil, errors.PermissionDenied
	}

	switch status.State {
	case data.TxnState_Decided:
		// is it trying to change prev decision?
		prevDecision := p.decisions[id]
		if prevDecision.Commit != cmd.Decision.Commit {
			return nil, errors.FailedPrecondition
		}

		// idempotent calls
		return status, nil
	case data.TxnState_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case data.TxnState_Prepared:
		p.decisions[id] = cmd.Decision

		status := newTxnStatus(id, cmd.Timestamp, data.TxnState_Decided)
		p.status[id] = status
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *txnProcessor) applyMarkTimedout(cmd *MarkTxnTimedout) (*data.TxnStatus, error) {
	id := NewTxnId(cmd.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, errors.NotFound
	} else if id.PrincipalPid != p.partitionID {
		return nil, errors.PermissionDenied
	}

	switch status.State {
	case data.TxnState_Timedout:
		if cmd.ReleaseLocks {
			delete(p.prepared, id)
		}

		return status, nil
	case data.TxnState_Prepared:
		status := newTxnStatus(id, cmd.Timestamp, data.TxnState_Timedout)
		p.status[id] = status
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *txnProcessor) applyBatch(index uint64, batch *TxnBatch) *TxnBatchResult {
	result := &TxnBatchResult{
		Autocommit:    make([]TxnStatus, len(batch.Autocommit)),
		Prepare:       make([]TxnStatus, len(batch.Prepare)),
		Commit:        make([]TxnStatus, len(batch.Commit)),
		Abort:         make([]TxnStatus, len(batch.Abort)),
		StoreDecision: make([]TxnStatus, len(batch.StoreDecision)),
		MarkTimedout:  make([]TxnStatus, len(batch.MarkTimedout)),
	}

	allWrites := make([]kv.Entry, 0, batch.ActionCount())

	for i, cmd := range batch.Autocommit {
		status, writes, err := p.applyAutocommit(cmd)
		allWrites = append(allWrites, writes...)
		result.Autocommit[i] = TxnStatus{status, err}
	}

	for i, cmd := range batch.Prepare {
		status, err := p.applyPrepare(cmd)
		result.Prepare[i] = TxnStatus{status, err}
	}

	for i, cmd := range batch.Commit {
		status, writes, err := p.applyCommit(cmd)
		allWrites = append(allWrites, writes...)
		result.Commit[i] = TxnStatus{status, err}
	}

	for i, cmd := range batch.Abort {
		status, err := p.applyAbort(cmd)
		result.Abort[i] = TxnStatus{status, err}
	}

	for i, cmd := range batch.StoreDecision {
		status, err := p.applyStoreDecision(cmd)
		result.Abort[i] = TxnStatus{status, err}
	}

	for i, cmd := range batch.MarkTimedout {
		status, err := p.applyMarkTimedout(cmd)
		result.MarkTimedout[i] = TxnStatus{status, err}
	}

	p.db.Put(index, allWrites...)
	return result
}

// Note: all validation checks below substract 1 from the timestamp parameter. This is a
// quick hack to enable idempotent behavior for the txn and made possible by the fact that
// HLC timestamps are strictly monotonic, synced with the control plane on session start,
// and persisted in the partition raft log.
//
// It is required that each action has repetable and deterministic execution behavior even
// when the underlying key-value store already contains the txn result/s. This scenario is
// expected to happen during normal service operation when a new replica joins the raft
// group: it will first fetch/stream an up-to-date key-value store checkpoint from another
// running replica, install it locally, then start applying the pending raft log entries.
// Unless a perfect in-sync, at the same applied raft index, kv store checkpoint and raft
// snapshot can be created it is almost certain that duplicate txn execution will happen.
func (p *txnProcessor) validate(id TxnId, timestamp uint64, txn *data.Txn) *data.TxnStatus {
	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Insert:
			addr := kv.Address{
				Keyspace:  x.Insert.Keyspace,
				Key:       x.Insert.Key,
				Timestamp: timestamp - 1,
			}

			if p.db.Exists(addr) {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyAlreadyExists)
			}
		case *data.Action_Update:
			addr := kv.Address{
				Keyspace:  x.Update.Keyspace,
				Key:       x.Update.Key,
				Timestamp: timestamp - 1,
			}

			if !p.db.Exists(addr) {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyNotFound)
			}
		case *data.Action_Upsert:
			// pass
		case *data.Action_Delete:
			addr := kv.Address{
				Keyspace:  x.Delete.Keyspace,
				Key:       x.Delete.Key,
				Timestamp: timestamp - 1,
			}

			if !p.db.Exists(addr) {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyNotFound)
			}
		case *data.Action_LockKey:
			if x.LockKey.Check == data.LockKey_None {
				continue
			}

			addr := kv.Address{
				Keyspace:  x.LockKey.Lock.Keyspace,
				Key:       x.LockKey.Lock.Key,
				Timestamp: timestamp - 1,
			}

			actual := p.db.Exists(addr)
			expected := x.LockKey.Check == data.LockKey_MustExist

			if actual != expected {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_LockCheckFailed)
			}
		case *data.Action_LockRange:
			if x.LockRange.Check == data.LockRange_None {
				continue
			}

			rang := mvcc.Range{
				Keyspace:  x.LockRange.Lock.Keyspace,
				StartKey:  x.LockRange.Lock.StartKey,
				EndKey:    x.LockRange.Lock.EndKey,
				Timestamp: timestamp - 1,
			}

			actual := p.db.ExistsInRange(rang)
			expected := x.LockRange.Check == data.LockRange_MustNotBeEmpty

			if actual != expected {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_LockCheckFailed)
			}
		}
	}

	return nil
}

func (p *txnProcessor) buildWriteEntries(timestamp uint64, txn *data.Txn) []kv.Entry {
	writes := make([]kv.Entry, 0, len(txn.Actions))

	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Insert:
			writes = append(writes, kv.Entry{
				Address: kv.NewAddress(x.Insert.Keyspace, x.Insert.Key, timestamp),
				Value:   mustEncodeValue(x.Insert.Value),
				Flags:   kv.FlagEmpty,
			})
		case *data.Action_Update:
			writes = append(writes, kv.Entry{
				Address: kv.NewAddress(x.Update.Keyspace, x.Update.Key, timestamp),
				Value:   mustEncodeValue(x.Update.Value),
				Flags:   kv.FlagEmpty,
			})
		case *data.Action_Upsert:
			writes = append(writes, kv.Entry{
				Address: kv.NewAddress(x.Upsert.Keyspace, x.Upsert.Key, timestamp),
				Value:   mustEncodeValue(x.Upsert.Value),
				Flags:   kv.FlagEmpty,
			})
		case *data.Action_Delete:
			writes = append(writes, kv.Entry{
				Address: kv.NewAddress(x.Delete.Keyspace, x.Delete.Key, timestamp),
				Flags:   kv.FlagTombstone,
			})
		}
	}

	return writes
}

func newTxnStatus(id TxnId, timestamp uint64, state data.TxnState) *data.TxnStatus {
	return &data.TxnStatus{
		Id:        id.ToProto(),
		Timestamp: timestamp,
		State:     state,
	}
}

func newTxnFailedStatus(id TxnId, timestamp uint64, actionId uint32, code data.ActionStatus_Code) *data.TxnStatus {
	return &data.TxnStatus{
		Id:        id.ToProto(),
		Timestamp: timestamp,
		State:     data.TxnState_Failed,
		ActionStatus: map[uint32]*data.ActionStatus{
			actionId: {
				Id:   actionId,
				Code: code,
			}},
	}
}
