package txn

import (
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
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

	allWrites := make([]mvcc.Entry, 0, batch.ActionCount())

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
	p.db.Put(index, allWrites...)

	p.maxTimestamp = max(p.maxTimestamp, batch.MaxTimestamp())

	// Ignores the HLC.Update error as log entries can be applied in various
	// scenarios from lagging group members to new members joining that need
	// to catch up with the leader, etc. The call here is mainly to ensure
	// that the newly-elected leader adopts a HCL timestamp greater than any
	// write timestamps issued by the previous leader/s.
	hlc.Update(p.maxTimestamp)
	return result
}

func (p *Manager) applyAutocommit(cmd *Autocommit) (*Status, []mvcc.Entry, error) {
	id := cmd.Txn.Id.id()
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case State_Committed, State_Failed:
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
	status = newStatus(id, cmd.Timestamp, State_Committed)

	p.status[id] = status
	return status, writes, nil
}

func (p *Manager) applyPrepare(cmd *Prepare) (*Status, error) {
	id := cmd.Txn.Id.id()
	status, ok := p.status[id]
	if ok {
		switch status.State {
		case State_Prepared, State_Failed:
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

	p.prepared[id] = &prepared{
		Txn:   cmd.Txn,
		Locks: locks,
	}

	status = newStatus(id, cmd.Timestamp, State_Prepared)

	p.status[id] = status
	return status, nil
}

func (p *Manager) applyCommit(cmd *Commit) (*Status, []mvcc.Entry, error) {
	id := cmd.Id.id()
	status, ok := p.status[id]
	if !ok {
		return nil, nil, errors.NotFound
	}

	switch status.State {
	case State_Committed:
		// idempotent calls
		return status, nil, nil
	case State_Prepared, State_Decided:
		prepared, ok := p.prepared[id]
		if !ok {
			return nil, nil, errors.NotFound
		}

		writes := p.buildWriteEntries(cmd.Timestamp, prepared.Txn)
		delete(p.prepared, id)
		delete(p.decisions, id)

		status := newStatus(id, cmd.Timestamp, State_Committed)
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

	switch status.State {
	case State_Aborted:
		// idempotent calls
		return status, nil
	case State_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case State_Prepared:
		if _, ok := p.prepared[id]; !ok {
			return nil, errors.NotFound
		}

		delete(p.prepared, id)
		delete(p.decisions, id) // presumed abort decisions are not stored, but delete just in case

		status := newStatus(id, cmd.Timestamp, State_Aborted)

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
	} else if id.PrincipalPid != p.partitionID {
		return nil, errors.PermissionDenied
	}

	switch status.State {
	case State_Decided:
		// is it trying to change prev decision?
		prevDecision := p.decisions[id]
		if prevDecision.Commit != cmd.Decision.Commit {
			return nil, errors.FailedPrecondition
		}

		// idempotent calls
		return status, nil
	case State_Timedout:
		// prepared txn was marked as timedout by the watchdog
		return status, nil
	case State_Prepared:
		p.decisions[id] = cmd.Decision

		status := newStatus(id, cmd.Timestamp, State_Decided)
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
	} else if id.PrincipalPid != p.partitionID {
		return nil, errors.PermissionDenied
	}

	switch status.State {
	case State_Timedout:
		if cmd.ReleaseLocks {
			delete(p.prepared, id)
		}

		return status, nil
	case State_Prepared:
		status := newStatus(id, cmd.Timestamp, State_Timedout)
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
			addr := kv.Address{
				Keyspace:  x.Insert.Keyspace,
				Key:       x.Insert.Key,
				Timestamp: timestamp - 1,
			}

			if p.db.Exists(addr) {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_KeyAlreadyExists)
			}
		case *Action_Update:
			addr := kv.Address{
				Keyspace:  x.Update.Keyspace,
				Key:       x.Update.Key,
				Timestamp: timestamp - 1,
			}

			if !p.db.Exists(addr) {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_KeyNotFound)
			}
		case *Action_Upsert:
			// pass
		case *Action_Delete:
			addr := kv.Address{
				Keyspace:  x.Delete.Keyspace,
				Key:       x.Delete.Key,
				Timestamp: timestamp - 1,
			}

			if !p.db.Exists(addr) {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_KeyNotFound)
			}
		case *Action_LockKey:
			if x.LockKey.Check == LockKey_None {
				continue
			}

			addr := kv.Address{
				Keyspace:  x.LockKey.Lock.Keyspace,
				Key:       x.LockKey.Lock.Key,
				Timestamp: timestamp - 1,
			}

			actual := p.db.Exists(addr)
			expected := x.LockKey.Check == LockKey_MustExist

			if actual != expected {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_LockCheckFailed)
			}
		case *Action_LockRange:
			if x.LockRange.Check == LockRange_None {
				continue
			}

			rang := mvcc.Range{
				Keyspace:  x.LockRange.Lock.Keyspace,
				StartKey:  x.LockRange.Lock.StartKey,
				EndKey:    x.LockRange.Lock.EndKey,
				Timestamp: timestamp - 1,
			}

			actual := p.db.ExistsInRange(rang)
			expected := x.LockRange.Check == LockRange_MustNotBeEmpty

			if actual != expected {
				return newFailedStatus(id, timestamp, action.Id, ActionStatus_LockCheckFailed)
			}
		}
	}

	return nil
}

func (p *Manager) buildWriteEntries(timestamp uint64, txn *Txn) []mvcc.Entry {
	writes := make([]mvcc.Entry, 0, len(txn.Actions))

	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *Action_Insert:
			writes = append(writes, mvcc.Entry{
				Address: kv.NewAddress(x.Insert.Keyspace, x.Insert.Key, timestamp),
				Value:   mustEncodeValue(x.Insert.Value),
				Flags:   mvcc.FlagEmpty,
			})
		case *Action_Update:
			writes = append(writes, mvcc.Entry{
				Address: kv.NewAddress(x.Update.Keyspace, x.Update.Key, timestamp),
				Value:   mustEncodeValue(x.Update.Value),
				Flags:   mvcc.FlagEmpty,
			})
		case *Action_Upsert:
			writes = append(writes, mvcc.Entry{
				Address: kv.NewAddress(x.Upsert.Keyspace, x.Upsert.Key, timestamp),
				Value:   mustEncodeValue(x.Upsert.Value),
				Flags:   mvcc.FlagEmpty,
			})
		case *Action_Delete:
			writes = append(writes, mvcc.Entry{
				Address: kv.NewAddress(x.Delete.Keyspace, x.Delete.Key, timestamp),
				Flags:   mvcc.FlagTombstone,
			})
		}
	}

	return writes
}

func newStatus(id id, timestamp uint64, state State) *Status {
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
		State:     State_Failed,
		ActionStatus: map[uint32]*ActionStatus{
			actionId: {
				Id:   actionId,
				Code: code,
			}},
	}
}
