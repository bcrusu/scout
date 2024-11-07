package txn

import (
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
)

func (p *Manager) ApplyBatch(index uint64, batch *data.TxnBatch) *BatchResults {
	p.lock.Lock()
	defer p.lock.Unlock()

	result := &BatchResults{
		Autocommit:    make([]BatchResult, len(batch.Autocommit)),
		Prepare:       make([]BatchResult, len(batch.Prepare)),
		Commit:        make([]BatchResult, len(batch.Commit)),
		Abort:         make([]BatchResult, len(batch.Abort)),
		StoreDecision: make([]BatchResult, len(batch.StoreDecision)),
		MarkTimedout:  make([]BatchResult, len(batch.MarkTimedout)),
	}

	allWrites := make([]mvcc.Record, 0, batch.ActionCount())

	for i, cmd := range batch.Autocommit {
		status, writes, err := p.applyAutocommit(cmd)
		allWrites = append(allWrites, writes...)
		result.Autocommit[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.Prepare {
		status, err := p.applyPrepare(cmd)
		result.Prepare[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.Commit {
		status, writes, err := p.applyCommit(cmd)
		allWrites = append(allWrites, writes...)
		result.Commit[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.Abort {
		status, err := p.applyAbort(cmd)
		result.Abort[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.StoreDecision {
		status, err := p.applyStoreDecision(cmd)
		result.StoreDecision[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.MarkTimedout {
		status, err := p.applyMarkTimedout(cmd)
		result.MarkTimedout[i] = BatchResult{status, err}
	}

	// call Put even when allWrites is empty to update the stored Raft index.
	p.db.Put(p.pid, index, allWrites...)

	p.maxTimestamp = max(p.maxTimestamp, batch.MaxTimestamp())

	// Ignores the HLC.Update error as log entries can be applied in various
	// scenarios from lagging group members to new members joining that need
	// to catch up with the leader, etc. The call here is mainly to ensure
	// that the newly-elected leader adopts a HCL timestamp greater than any
	// write timestamps issued by the previous leader/s.
	hlc.Update(p.maxTimestamp)

	p.cleanup()
	return result
}

func (p *Manager) applyAutocommit(cmd *data.Autocommit) (*data.TxnStatus, []mvcc.Record, error) {
	id := newId(cmd.Txn.Id)
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case data.TxnStatus_Committed, data.TxnStatus_Failed:
			// idempotent calls
			return status, nil, nil
		default:
			return nil, nil, errors.FailedPrecondition
		}
	}

	locks := cmd.Txn.BuildLocks()
	status = p.acquireLocks(id, cmd.Timestamp, locks)
	if status != nil {
		return status, nil, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		return status, nil, nil
	}

	writes := p.buildWriteEntries(cmd.Timestamp, cmd.Txn)
	status = newStatus(id, cmd.Timestamp, data.TxnStatus_Committed)

	p.status[id] = status
	p.metrics.Tracked.Add(1)
	p.metrics.Autocommitted.Add(1)
	return status, writes, nil
}

func (p *Manager) applyPrepare(cmd *data.Prepare) (*data.TxnStatus, error) {
	id := newId(cmd.Txn.Id)
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case data.TxnStatus_Prepared, data.TxnStatus_Failed:
			// idempotent calls
			return status, nil
		default:
			return nil, errors.FailedPrecondition
		}
	}

	locks := cmd.Txn.BuildLocks()
	status = p.acquireLocks(id, cmd.Timestamp, locks)
	if status != nil {
		return status, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		return status, nil
	}

	status = newStatus(id, cmd.Timestamp, data.TxnStatus_Prepared)

	p.prepared[id] = &data.Prepared{
		Txn:   cmd.Txn,
		Locks: locks,
	}

	p.status[id] = status
	p.metrics.Prepared.Add(1)
	p.metrics.Running.Add(1)
	p.metrics.Tracked.Add(1)
	return status, nil
}

func (p *Manager) applyCommit(cmd *data.Commit) (*data.TxnStatus, []mvcc.Record, error) {
	id := newId(cmd.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, nil, errors.NotFound
	}

	prepared, ok := p.prepared[id]
	if !ok {
		p.log.Warn("Commit: prepared txn not found.", "id", id)
		return nil, nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Committed:
		// idempotent calls
		return status, nil, nil
	case data.TxnStatus_Prepared, data.TxnStatus_Decided:
		p.releaseLocks(prepared)
		writes := p.buildWriteEntries(cmd.Timestamp, prepared.Txn)

		status := newStatus(id, cmd.Timestamp, data.TxnStatus_Committed)
		// stores the list of participants to be able to recompose the txn results
		// after the initial client call returns.
		status.ParticipantPids = prepared.Txn.ParticipantPids

		p.status[id] = status
		p.metrics.Running.Add(-1)
		p.metrics.Committed.Add(1)
		return status, writes, nil
	default:
		return nil, nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyAbort(cmd *data.Abort) (*data.TxnStatus, error) {
	id := newId(cmd.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, errors.NotFound
	}

	prepared, ok := p.prepared[id]
	if !ok {
		p.log.Warn("Abort: prepared txn not found.", "id", id)
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Aborted:
		// idempotent calls
		return status, nil
	case data.TxnStatus_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case data.TxnStatus_Prepared:
		p.releaseLocks(prepared)
		status := newStatus(id, cmd.Timestamp, data.TxnStatus_Aborted)

		p.status[id] = status
		p.metrics.Running.Add(-1)
		p.metrics.Aborted.Add(1)
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyStoreDecision(cmd *data.StoreDecision) (*data.TxnStatus, error) {
	id := newId(cmd.Decision.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, errors.NotFound
	} else if id.PrincipalPid != p.pid {
		return nil, errors.PermissionDenied
	}

	prepared, ok := p.prepared[id]
	if !ok {
		p.log.Warn("StoreDecision: prepared txn not found.", "id", id)
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Decided:
		// is it trying to change prev decision?
		if prepared.Decision.Commit != cmd.Decision.Commit {
			return nil, errors.FailedPrecondition
		}

		// idempotent calls
		return status, nil
	case data.TxnStatus_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case data.TxnStatus_Prepared:
		prepared.Decision = cmd.Decision
		status := newStatus(id, cmd.Timestamp, data.TxnStatus_Decided)

		p.status[id] = status
		p.metrics.Decided.Add(1)
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyMarkTimedout(cmd *data.MarkTimedout) (*data.TxnStatus, error) {
	id := newId(cmd.Id)
	status, ok := p.status[id]
	if !ok {
		return nil, errors.NotFound
	} else if id.PrincipalPid != p.pid {
		return nil, errors.PermissionDenied
	}

	prepared, ok := p.prepared[id]
	if !ok {
		p.log.Warn("MarkTimedout: prepared txn not found.", "id", id)
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Timedout:
		if cmd.ReleaseLocks {
			p.releaseLocks(prepared)
		}

		return status, nil
	case data.TxnStatus_Prepared:
		status := newStatus(id, cmd.Timestamp, data.TxnStatus_Timedout)

		p.status[id] = status
		p.metrics.Running.Add(-1)
		p.metrics.Timedout.Add(1)
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
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
func (p *Manager) validate(id id, timestamp uint64, txn *data.Txn) *data.TxnStatus {
	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Insert:
			addr := mvcc.NewAddress(x.Insert.Keyspace, x.Insert.Key)

			if p.db.Exists(p.pid, timestamp-1, addr) {
				p.metrics.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyAlreadyExists)
			}
		case *data.Action_Update:
			addr := mvcc.NewAddress(x.Update.Keyspace, x.Update.Key)

			if !p.db.Exists(p.pid, timestamp-1, addr) {
				p.metrics.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyNotFound)
			}
		case *data.Action_Upsert:
			// pass
		case *data.Action_Delete:
			addr := mvcc.NewAddress(x.Delete.Keyspace, x.Delete.Key)

			if !p.db.Exists(p.pid, timestamp-1, addr) {
				p.metrics.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyNotFound)
			}
		case *data.Action_LockKey:
			if x.LockKey.Check == data.LockKey_None {
				continue
			}

			addr := mvcc.NewAddress(x.LockKey.Lock.Keyspace, x.LockKey.Lock.Key)

			actual := p.db.Exists(p.pid, timestamp-1, addr)
			expected := x.LockKey.Check == data.LockKey_MustExist

			if actual != expected {
				p.metrics.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_LockCheckFailed)
			}
		case *data.Action_LockRange:
			if x.LockRange.Check == data.LockRange_None {
				continue
			}

			start := mvcc.NewAddress(x.LockRange.Lock.Keyspace, x.LockRange.Lock.StartKey)
			end := mvcc.NewAddress(x.LockRange.Lock.Keyspace, x.LockRange.Lock.EndKey)

			actual := p.db.ExistsInRange(p.pid, timestamp-1, start, end)
			expected := x.LockRange.Check == data.LockRange_MustNotBeEmpty

			if actual != expected {
				p.metrics.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_LockCheckFailed)
			}
		}
	}

	return nil
}

func (p *Manager) buildWriteEntries(timestamp uint64, txn *data.Txn) []mvcc.Record {
	writes := make([]mvcc.Record, 0, len(txn.Actions))

	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Insert:
			writes = append(writes, mvcc.Record{
				Address:   mvcc.NewAddress(x.Insert.Keyspace, x.Insert.Key),
				Timestamp: timestamp,
				Value:     mustEncodeValue(x.Insert.Value),
				Flags:     mvcc.FlagEmpty,
			})
		case *data.Action_Update:
			writes = append(writes, mvcc.Record{
				Address:   mvcc.NewAddress(x.Update.Keyspace, x.Update.Key),
				Timestamp: timestamp,
				Value:     mustEncodeValue(x.Update.Value),
				Flags:     mvcc.FlagEmpty,
			})
		case *data.Action_Upsert:
			writes = append(writes, mvcc.Record{
				Address:   mvcc.NewAddress(x.Upsert.Keyspace, x.Upsert.Key),
				Timestamp: timestamp,
				Value:     mustEncodeValue(x.Upsert.Value),
				Flags:     mvcc.FlagEmpty,
			})
		case *data.Action_Delete:
			writes = append(writes, mvcc.Record{
				Address:   mvcc.NewAddress(x.Delete.Keyspace, x.Delete.Key),
				Timestamp: timestamp,
				Flags:     mvcc.FlagTombstone,
			})
		}
	}

	return writes
}

func (p *Manager) cleanup() {
	oldest := hlc.FromTime(time.Now().Add(-p.cleanAfter))
	toRemove := map[id]bool{}

	for id, status := range p.status {
		if status.Timestamp > oldest || !status.State.IsFinal() {
			continue
		} else if x := p.prepared[id]; x != nil && !x.LocksReleased {
			continue
		}
		toRemove[id] = true
	}

	for id := range toRemove {
		delete(p.status, id)
		delete(p.prepared, id)
	}

	p.metrics.Tracked.Add(-len(toRemove))
}
