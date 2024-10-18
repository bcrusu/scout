package txn

import (
	"time"

	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
)

func (p *Manager) ApplyBatch(index uint64, batch *Batch) *BatchResults {
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
		result.Abort[i] = BatchResult{status, err}
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

func (p *Manager) applyAutocommit(cmd *Autocommit) (*Status, []mvcc.Record, error) {
	id := cmd.Txn.Id.id()
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case Status_Committed, Status_Failed:
			// idempotent calls
			return status, nil, nil
		default:
			return nil, nil, errors.FailedPrecondition
		}
	}

	locks := cmd.Txn.BuildLocks()
	status = p.checkLocks(id, cmd.Timestamp, locks)
	if status != nil {
		return status, nil, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		return status, nil, nil
	}

	writes := p.buildWriteEntries(cmd.Timestamp, cmd.Txn)
	status = newStatus(id, cmd.Timestamp, Status_Committed)

	p.status[id] = status
	return status, writes, nil
}

func (p *Manager) applyPrepare(cmd *Prepare) (*Status, error) {
	id := cmd.Txn.Id.id()
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case Status_Prepared, Status_Failed:
			// idempotent calls
			return status, nil
		default:
			return nil, errors.FailedPrecondition
		}
	}

	locks := cmd.Txn.BuildLocks()
	status = p.checkLocks(id, cmd.Timestamp, locks)
	if status != nil {
		return status, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		return status, nil
	}

	p.prepared[id] = &Prepared{
		Txn:   cmd.Txn,
		Locks: locks,
	}

	status = newStatus(id, cmd.Timestamp, Status_Prepared)

	p.status[id] = status
	return status, nil
}

func (p *Manager) applyCommit(cmd *Commit) (*Status, []mvcc.Record, error) {
	id := cmd.Id.id()
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
	case Status_Committed:
		// idempotent calls
		return status, nil, nil
	case Status_Prepared, Status_Decided:
		prepared.ReleaseLocks()
		writes := p.buildWriteEntries(cmd.Timestamp, prepared.Txn)

		status := newStatus(id, cmd.Timestamp, Status_Committed)
		// stores the list of participants to be able to recompose the txn results
		// after the initial client call returns.
		status.ParticipantPids = prepared.Txn.ParticipantPids

		p.status[id] = status
		return status, writes, nil
	default:
		return nil, nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyAbort(cmd *Abort) (*Status, error) {
	id := cmd.Id.id()
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
	case Status_Aborted:
		// idempotent calls
		return status, nil
	case Status_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case Status_Prepared:
		prepared.ReleaseLocks()
		status := newStatus(id, cmd.Timestamp, Status_Aborted)

		p.status[id] = status
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyStoreDecision(cmd *StoreDecision) (*Status, error) {
	id := cmd.Decision.Id.id()
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
	case Status_Decided:
		// is it trying to change prev decision?
		if prepared.Decision.Commit != cmd.Decision.Commit {
			return nil, errors.FailedPrecondition
		}

		// idempotent calls
		return status, nil
	case Status_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case Status_Prepared:
		prepared.Decision = cmd.Decision

		status := newStatus(id, cmd.Timestamp, Status_Decided)
		p.status[id] = status
		return status, nil
	default:
		return nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyMarkTimedout(cmd *MarkTimedout) (*Status, error) {
	id := cmd.Id.id()
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
	case Status_Timedout:
		if cmd.ReleaseLocks {
			prepared.ReleaseLocks()
		}

		return status, nil
	case Status_Prepared:
		status := newStatus(id, cmd.Timestamp, Status_Timedout)
		p.status[id] = status
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
func (p *Manager) validate(id id, timestamp uint64, txn *Txn) *Status {
	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *Action_Insert:
			addr := mvcc.NewAddress(x.Insert.Keyspace, x.Insert.Key)

			if p.db.Exists(p.pid, timestamp-1, addr) {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_KeyAlreadyExists)
			}
		case *Action_Update:
			addr := mvcc.NewAddress(x.Update.Keyspace, x.Update.Key)

			if !p.db.Exists(p.pid, timestamp-1, addr) {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_KeyNotFound)
			}
		case *Action_Upsert:
			// pass
		case *Action_Delete:
			addr := mvcc.NewAddress(x.Delete.Keyspace, x.Delete.Key)

			if !p.db.Exists(p.pid, timestamp-1, addr) {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_KeyNotFound)
			}
		case *Action_LockKey:
			if x.LockKey.Check == LockKey_None {
				continue
			}

			addr := mvcc.NewAddress(x.LockKey.Lock.Keyspace, x.LockKey.Lock.Key)

			actual := p.db.Exists(p.pid, timestamp-1, addr)
			expected := x.LockKey.Check == LockKey_MustExist

			if actual != expected {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_LockCheckFailed)
			}
		case *Action_LockRange:
			if x.LockRange.Check == LockRange_None {
				continue
			}

			start := mvcc.NewAddress(x.LockRange.Lock.Keyspace, x.LockRange.Lock.StartKey)
			end := mvcc.NewAddress(x.LockRange.Lock.Keyspace, x.LockRange.Lock.EndKey)

			actual := p.db.ExistsInRange(p.pid, timestamp-1, start, end)
			expected := x.LockRange.Check == LockRange_MustNotBeEmpty

			if actual != expected {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_LockCheckFailed)
			}
		}
	}

	return nil
}

func (p *Manager) buildWriteEntries(timestamp uint64, txn *Txn) []mvcc.Record {
	writes := make([]mvcc.Record, 0, len(txn.Actions))

	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *Action_Insert:
			writes = append(writes, mvcc.Record{
				Address:   mvcc.NewAddress(x.Insert.Keyspace, x.Insert.Key),
				Timestamp: timestamp,
				Value:     mustEncodeValue(x.Insert.Value),
				Flags:     mvcc.FlagEmpty,
			})
		case *Action_Update:
			writes = append(writes, mvcc.Record{
				Address:   mvcc.NewAddress(x.Update.Keyspace, x.Update.Key),
				Timestamp: timestamp,
				Value:     mustEncodeValue(x.Update.Value),
				Flags:     mvcc.FlagEmpty,
			})
		case *Action_Upsert:
			writes = append(writes, mvcc.Record{
				Address:   mvcc.NewAddress(x.Upsert.Keyspace, x.Upsert.Key),
				Timestamp: timestamp,
				Value:     mustEncodeValue(x.Upsert.Value),
				Flags:     mvcc.FlagEmpty,
			})
		case *Action_Delete:
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
}

func newStatus(id id, timestamp uint64, state Status_State) *Status {
	return &Status{
		Id:        id.ToProto(),
		Timestamp: timestamp,
		State:     state,
	}
}

func newFailedStatus(id id, timestamp uint64, actionId uint32, code ActionStatus_Code) *Status {
	return &Status{
		Id:        id.ToProto(),
		Timestamp: timestamp,
		State:     Status_Failed,
		ActionStatus: map[uint32]*ActionStatus{
			actionId: {
				Id:   actionId,
				Code: code,
			}},
	}
}
