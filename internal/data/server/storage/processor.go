package storage

import (
	"encoding/base64"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/config"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
)

var (
	logT = logging.WithComponent("storage_txn").NoContext()
)

// TODO: prune status for old txn
type txnProcessor struct {
	partitionID uint32
	db          *dbBreaker
	status      map[TxnId]*data.TxnStatus
	prepared    map[TxnId]*txnLocks
	decisions   map[TxnId]*data.TxnDecision
}

type txnLocks struct {
	Txn   *data.Txn
	Locks []*Lock
}

func newTxnProcessor(partitionID uint32, db DB) *txnProcessor {
	cdb := &dbBreaker{
		db:          db,
		retryPolicy: config.Get().DBRetryPolicy,
	}

	return &txnProcessor{
		partitionID: partitionID,
		db:          cdb,
		status:      map[TxnId]*data.TxnStatus{},
		prepared:    map[TxnId]*txnLocks{},
	}
}

func (p *txnProcessor) applyAutocommit(cmd *TxnAutocommit) (*data.TxnStatus, error) {
	id := NewTxnId(cmd.Txn.Id)
	status, ok := p.status[id]
	if !ok {
		status = p.autocommit(id, cmd.Timestamp, cmd.Txn)
		p.status[id] = status
		return status, nil
	}

	switch status.State {
	case data.TxnState_Committed, data.TxnState_Failed:
		// idempotent calls
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *txnProcessor) applyPrepare(cmd *TxnPrepare) (*data.TxnStatus, error) {
	id := NewTxnId(cmd.Txn.Id)
	status, ok := p.status[id]
	if !ok {
		status = p.prepare(id, cmd.Timestamp, cmd.Txn)
		p.status[id] = status
		return status, nil
	}

	switch status.State {
	case data.TxnState_Prepared, data.TxnState_Failed:
		// idempotent calls
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *txnProcessor) applyCommit(cmd *TxnCommit) (*data.TxnStatus, error) {
	id := NewTxnId(cmd.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnState_Committed:
		// idempotent calls
		return status, nil
	case data.TxnState_Prepared, data.TxnState_Decided:
		status, err := p.commit(id, cmd.Timestamp)
		if err != nil {
			return nil, err
		}
		p.status[id] = status
		return status, nil
	default:
		return nil, errors.FailedPrecondition
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
		status, err := p.abort(id, cmd.Timestamp)
		if err != nil {
			return nil, err
		}
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

		status := newTxnStatus(id, cmd.Timestamp, data.TxnState_Decided, nil)
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
		status := newTxnStatus(id, cmd.Timestamp, data.TxnState_Timedout, nil)
		p.status[id] = status
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

// TODO: parallel execution
func (p *txnProcessor) applyBatch(batch *TxnBatch) *TxnBatchResult {
	result := &TxnBatchResult{
		Autocommit:    make([]TxnStatus, len(batch.Autocommit)),
		Prepare:       make([]TxnStatus, len(batch.Prepare)),
		Commit:        make([]TxnStatus, len(batch.Commit)),
		Abort:         make([]TxnStatus, len(batch.Abort)),
		StoreDecision: make([]TxnStatus, len(batch.StoreDecision)),
		MarkTimedout:  make([]TxnStatus, len(batch.MarkTimedout)),
	}

	for i, cmd := range batch.Autocommit {
		status, err := p.applyAutocommit(cmd)
		result.Autocommit[i] = TxnStatus{status, err}
	}

	for i, cmd := range batch.Prepare {
		status, err := p.applyPrepare(cmd)
		result.Prepare[i] = TxnStatus{status, err}
	}

	for i, cmd := range batch.Commit {
		status, err := p.applyCommit(cmd)
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

	return result
}

func (p *txnProcessor) autocommit(id TxnId, timestamp uint64, txn *data.Txn) *data.TxnStatus {
	locks := buildLocks(txn)
	status := p.checkLocks(id, timestamp, locks)
	if status != nil {
		return status
	}

	status = p.validate(id, timestamp, txn)
	if status != nil {
		return status
	}

	results := p.doActions(timestamp, txn)

	return newTxnStatus(id, timestamp, data.TxnState_Committed, results)
}

func (p *txnProcessor) prepare(id TxnId, timestamp uint64, txn *data.Txn) *data.TxnStatus {
	locks := buildLocks(txn)
	status := p.checkLocks(id, timestamp, locks)
	if status != nil {
		return status
	}

	status = p.validate(id, timestamp, txn)
	if status != nil {
		return status
	}

	p.prepared[id] = &txnLocks{
		Txn:   txn,
		Locks: locks,
	}

	return newTxnStatus(id, timestamp, data.TxnState_Prepared, nil)
}

func (p *txnProcessor) commit(id TxnId, timestamp uint64) (*data.TxnStatus, error) {
	prepared, ok := p.prepared[id]
	if !ok {
		return nil, errors.NotFound
	}

	results := p.doActions(timestamp, prepared.Txn)
	delete(p.prepared, id)
	delete(p.decisions, id)

	status := newTxnStatus(id, timestamp, data.TxnState_Committed, results)
	// stores the list of participants to be able to recompose the txn results
	// after the initial client call returns.
	status.ParticipantPids = prepared.Txn.ParticipantPids
	return status, nil
}

func (p *txnProcessor) abort(id TxnId, timestamp uint64) (*data.TxnStatus, error) {
	if _, ok := p.prepared[id]; !ok {
		return nil, errors.NotFound
	}

	delete(p.prepared, id)
	delete(p.decisions, id) // presumed abort decisions are not stored, but delete just in case

	return newTxnStatus(id, timestamp, data.TxnState_Aborted, nil), nil
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
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Insert.Keyspace,
				Key:         x.Insert.Key,
				Timestamp:   timestamp - 1,
			}

			if p.db.Exists(loc) {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyAlreadyExists)
			}
		case *data.Action_Update:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Update.Keyspace,
				Key:         x.Update.Key,
				Timestamp:   timestamp - 1,
			}

			if !p.db.Exists(loc) {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyNotFound)
			}
		case *data.Action_Upsert:
			// pass
		case *data.Action_Delete:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Delete.Keyspace,
				Key:         x.Delete.Key,
				Timestamp:   timestamp - 1,
			}

			if !p.db.Exists(loc) {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyNotFound)
			}
		case *data.Action_LockKey:
			if x.LockKey.Check == data.LockKey_None {
				continue
			}

			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.LockKey.Lock.Keyspace,
				Key:         x.LockKey.Lock.Key,
				Timestamp:   timestamp - 1,
			}

			actual := p.db.Exists(loc)
			expected := x.LockKey.Check == data.LockKey_MustExist

			if actual != expected {
				return newTxnFailedStatus(id, timestamp, action.Id, data.ActionStatus_LockCheckFailed)
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
				Timestamp:   timestamp - 1,
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

func (p *txnProcessor) doActions(timestamp uint64, txn *data.Txn) map[uint32]*data.ActionStatus {
	result := map[uint32]*data.ActionStatus{}

	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Read:
			ts := timestamp
			if x.Read.Timestamp != 0 {
				ts = x.Read.Timestamp
			}

			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Read.Keyspace,
				Key:         x.Read.Key,
				Timestamp:   ts,
			}

			if valueAt := p.db.Get(loc); valueAt == nil {
				result[action.Id] = newActionStatus(action.Id, data.ActionStatus_KeyNotFound, nil)
			} else if value, err := decodeValue(valueAt.Data); err != nil {
				str := base64.RawURLEncoding.EncodeToString(valueAt.Data)
				logT.WithError(err).Error("Failed to decode.", "value", str, "value_timestamp", timestamp, "location", loc)
				result[action.Id] = newActionStatus(action.Id, data.ActionStatus_CorruptedData, nil)
			} else {
				result[action.Id] = newActionStatus(action.Id, data.ActionStatus_Success, value)
			}
		case *data.Action_Insert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Insert.Keyspace,
				Key:         x.Insert.Key,
				Timestamp:   timestamp,
			}

			p.db.Set(loc, mustEncodeValue(x.Insert.Value))
		case *data.Action_Update:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Update.Keyspace,
				Key:         x.Update.Key,
				Timestamp:   timestamp,
			}

			p.db.Set(loc, mustEncodeValue(x.Update.Value))
		case *data.Action_Upsert:
			loc := Location{
				PartitionID: p.partitionID,
				Keyspace:    x.Upsert.Keyspace,
				Key:         x.Upsert.Key,
				Timestamp:   timestamp,
			}

			p.db.Set(loc, mustEncodeValue(x.Upsert.Value))
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

	return result
}

func newTxnStatus(id TxnId, timestamp uint64, state data.TxnState, results map[uint32]*data.ActionStatus) *data.TxnStatus {
	return &data.TxnStatus{
		Id:           id.ToProto(),
		Timestamp:    timestamp,
		State:        state,
		ActionStatus: results,
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

func newActionStatus(id uint32, code data.ActionStatus_Code, value *data.Value) *data.ActionStatus {
	return &data.ActionStatus{
		Id:       id,
		Code:     code,
		ValueRef: nil,
		Value:    value, // TODO: set only ValueRef to avoid the Value being written to the Raft log. Later, the actual value bytes can be fetched from db
	}
}
