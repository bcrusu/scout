package txn

import (
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/logging"
)

type completion func()

// Applies a batch of txn operations. The order of operations is important for
// performance reasons: first process the Abort/MarkTimedout/Commit ops which
// will release held locks, then Autocommit ops which will not aquire new locks,
// and finally Prepare ops where new locks are acquired.
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

	var completions []completion
	allWrites := make([]mvcc.Record, 0, batch.ActionCount())

	appendCompletion := func(completion completion) {
		if completion != nil {
			completions = append(completions, completion)
		}
	}

	for i, cmd := range batch.Abort {
		status, err := p.applyAbort(cmd)
		result.Abort[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.MarkTimedout {
		status, err := p.applyMarkTimedout(cmd)
		result.MarkTimedout[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.Commit {
		status, writes, completion, err := p.applyCommit(cmd)
		if err == nil {
			appendCompletion(completion)
			allWrites = append(allWrites, writes...)
		}
		result.Commit[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.Autocommit {
		status, writes, completion, err := p.applyAutocommit(cmd)
		if err == nil {
			appendCompletion(completion)
			allWrites = append(allWrites, writes...)
		}
		result.Autocommit[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.Prepare {
		status, err := p.applyPrepare(cmd)
		result.Prepare[i] = BatchResult{status, err}
	}

	for i, cmd := range batch.StoreDecision {
		status, err := p.applyStoreDecision(cmd)
		result.StoreDecision[i] = BatchResult{status, err}
	}

	// call Put even when allWrites is empty to update the stored Raft index.
	p.db.Put(p.pid, index, allWrites...)

	for _, completion := range completions {
		completion()
	}

	p.maxTimestamp = batch.MaxTimestamp

	p.cleanup()
	return result
}

func (p *Manager) applyAutocommit(cmd *data.Autocommit) (*data.TxnStatus, []mvcc.Record, completion, error) {
	id := newId(cmd.Txn.Id)
	log := p.log.WithTrace(cmd.Trace).With("cmd", "Autocommit", "id", id, "ts", cmd.Timestamp)

	status, ok := p.status[id]
	if ok {
		switch status.State {
		case data.TxnStatus_Committed, data.TxnStatus_Failed:
			log.Debug("Duplicate write.")
			return status, nil, nil, nil
		default:
			log.Error("Invalid status.", "state", status.State)
			return nil, nil, nil, errors.FailedPrecondition
		}
	}

	locks := cmd.Txn.BuildLocks()
	status = p.checkLocks(id, cmd.Timestamp, locks)
	if status != nil {
		log.Trace("Lock failed.")
		return status, nil, nil, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		log.Trace("Validation failed.")
		return status, nil, nil, nil
	}

	writes := p.buildWriteRecords(cmd.Timestamp, cmd.Txn)
	status = newStatus(id, cmd.Timestamp, data.TxnStatus_Committed)

	p.status[id] = status
	p.meters.Tracked.Add(1)

	completion := func() {
		p.logRecords(log, writes)
		log.Trace("Success.")
	}

	return status, writes, completion, nil
}

func (p *Manager) applyPrepare(cmd *data.Prepare) (*data.TxnStatus, error) {
	id := newId(cmd.Txn.Id)
	log := p.log.WithTrace(cmd.Trace).With("cmd", "Prepare", "id", id, "ts", cmd.Timestamp)

	status, ok := p.status[id]
	if ok {
		switch status.State {
		case data.TxnStatus_Prepared, data.TxnStatus_Failed:
			log.Debug("Duplicate write.")
			return status, nil
		default:
			log.Error("Invalid status.", "state", status.State)
			return nil, errors.FailedPrecondition
		}
	}

	locks := cmd.Txn.BuildLocks()
	status = p.checkLocks(id, cmd.Timestamp, locks)
	if status != nil {
		log.Trace("Lock failed.")
		return status, nil
	}

	status = p.validate(id, cmd.Timestamp, cmd.Txn)
	if status != nil {
		log.Trace("Validation failed.")
		return status, nil
	}

	status = newStatus(id, cmd.Timestamp, data.TxnStatus_Prepared)

	p.prepared[id] = &data.Prepared{
		Txn:       cmd.Txn,
		Timestamp: cmd.Timestamp,
		Locks:     locks,
	}

	p.status[id] = status
	p.meters.Running.Add(1)
	p.meters.Tracked.Add(1)
	p.meters.LocksHeld.Add(len(locks))
	log.Trace("Success.")
	return status, nil
}

func (p *Manager) applyCommit(cmd *data.Commit) (*data.TxnStatus, []mvcc.Record, completion, error) {
	id := newId(cmd.Id)
	log := p.log.WithTrace(cmd.Trace).With("cmd", "Commit", "id", id, "ts", cmd.Timestamp)

	status, ok := p.status[id]
	if !ok {
		log.Error("Status not found.")
		return nil, nil, nil, errors.NotFound
	}

	prepared, ok := p.prepared[id]
	if !ok {
		log.Error("Prepared not found.")
		return nil, nil, nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Committed:
		log.Debug("Duplicate write.")
		return status, nil, nil, nil
	case data.TxnStatus_Decided:
		// the principal partition stores the txn decision...
		if cmd.Timestamp != prepared.Decision.CommitTimestamp {
			log.Error("Detected commit timestamp different than decision timestamp.")
			return nil, nil, nil, errors.InvalidRequest
		}
	case data.TxnStatus_Prepared:
		// ... while all other participant partitions follow the decision
		if cmd.Timestamp < status.Timestamp {
			log.Error("Detected commit timestamp before prepared timestamp.")
			return nil, nil, nil, errors.InvalidRequest
		}
	default:
		log.Error("Invalid status.", "state", status.State)
		return nil, nil, nil, errors.FailedPrecondition
	}

	writes := p.buildWriteRecords(cmd.Timestamp, prepared.Txn)

	status = newStatus(id, cmd.Timestamp, data.TxnStatus_Committed)
	// stores the list of participants to be able to recompose the txn results
	// after the initial client call returns.
	status.ParticipantPids = prepared.Txn.ParticipantPids

	p.status[id] = status

	completion := func() {
		p.logRecords(log, writes)
		p.releaseLocks(prepared)
		p.meters.Running.Add(-1)
		log.Trace("Success.")
	}

	return status, writes, completion, nil
}

func (p *Manager) applyAbort(cmd *data.Abort) (*data.TxnStatus, error) {
	id := newId(cmd.Id)
	log := p.log.WithTrace(cmd.Trace).With("cmd", "Abort", "id", id, "ts", cmd.Timestamp)

	status, ok := p.status[id]
	if !ok {
		log.Error("Status not found.")
		return nil, errors.NotFound
	}

	prepared, ok := p.prepared[id]
	if !ok {
		log.Error("Prepared not found.")
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Aborted:
		log.Debug("Duplicate write.")
		return status, nil
	case data.TxnStatus_Timedout:
		log.Trace("Transaction was already marked as timedout.")
		return status, nil
	case data.TxnStatus_Prepared:
		p.releaseLocks(prepared)
		status := newStatus(id, cmd.Timestamp, data.TxnStatus_Aborted)

		p.status[id] = status
		p.meters.Running.Add(-1)
		log.Trace("Success.")
		return status, nil
	default:
		log.Error("Invalid status.", "state", status.State)
		return nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyStoreDecision(cmd *data.StoreDecision) (*data.TxnStatus, error) {
	id := newId(cmd.Decision.Id)
	log := p.log.WithTrace(cmd.Trace).With("cmd", "StoreDecision", "id", id, "ts", cmd.Timestamp, "commit_ts", cmd.Decision.CommitTimestamp)

	status, ok := p.status[id]
	if !ok {
		log.Error("Status not found.")
		return nil, errors.NotFound
	} else if id.PrincipalPid != p.pid {
		log.Error("Not principal partition.")
		return nil, errors.PermissionDenied
	}

	prepared, ok := p.prepared[id]
	if !ok {
		log.Error("Prepared not found.")
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Decided:
		if prepared.Decision.Commit != cmd.Decision.Commit {
			log.Error("Tried to change previous decision.")
			return nil, errors.FailedPrecondition
		}

		log.Debug("Duplicate write.")
		return status, nil
	case data.TxnStatus_Timedout:
		log.Trace("Transaction was already marked as timedout.")
		return status, nil
	case data.TxnStatus_Prepared:
		if cmd.Decision.CommitTimestamp < status.Timestamp {
			log.Error("Detected commit timestamp before prepared timestamp.")
			return nil, errors.InvalidRequest
		}

		prepared.Decision = cmd.Decision
		status := newStatus(id, cmd.Timestamp, data.TxnStatus_Decided)

		p.status[id] = status
		log.Trace("Success.")
		return status, nil
	default:
		log.Error("Invalid status.", "state", status.State)
		return nil, errors.FailedPrecondition
	}
}

func (p *Manager) applyMarkTimedout(cmd *data.MarkTimedout) (*data.TxnStatus, error) {
	id := newId(cmd.Id)
	log := p.log.WithTrace(cmd.Trace).With("cmd", "MarkTimedout", "id", id, "ts", cmd.Timestamp, "release", cmd.ReleaseLocks)

	status, ok := p.status[id]
	if !ok {
		log.Error("Status not found.")
		return nil, errors.NotFound
	} else if id.PrincipalPid != p.pid {
		log.Error("Not principal partition.")
		return nil, errors.PermissionDenied
	}

	prepared, ok := p.prepared[id]
	if !ok {
		log.Error("Prepared not found.")
		return nil, errors.NotFound
	}

	switch status.State {
	case data.TxnStatus_Timedout:
		if cmd.ReleaseLocks {
			p.releaseLocks(prepared)
			log.Trace("Released locks.")
		}

		return status, nil
	case data.TxnStatus_Prepared:
		status := newStatus(id, cmd.Timestamp, data.TxnStatus_Timedout)

		p.status[id] = status
		p.meters.Running.Add(-1)
		p.meters.Timedout.Add(1)
		log.Trace("Marked.")
		return status, nil
	default:
		log.Error("Invalid status.", "state", status.State)
		return nil, errors.FailedPrecondition
	}
}

// Note: all validation checks below subtract 1 from the timestamp parameter. This is a
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
				p.meters.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyAlreadyExists)
			}
		case *data.Action_Update:
			addr := mvcc.NewAddress(x.Update.Keyspace, x.Update.Key)

			if !p.db.Exists(p.pid, timestamp-1, addr) {
				p.meters.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_KeyNotFound)
			}
		case *data.Action_Upsert:
			// pass
		case *data.Action_Delete:
			addr := mvcc.NewAddress(x.Delete.Keyspace, x.Delete.Key)

			if !p.db.Exists(p.pid, timestamp-1, addr) {
				p.meters.ValidationFailed.Add(1)
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
				p.meters.ValidationFailed.Add(1)
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
				p.meters.ValidationFailed.Add(1)
				return newFailedStatus(id, timestamp, action.Id, data.ActionStatus_LockCheckFailed)
			}
		}
	}

	return nil
}

func (p *Manager) buildWriteRecords(timestamp uint64, txn *data.Txn) []mvcc.Record {
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

	p.meters.Tracked.Add(-len(toRemove))
}

func (p *Manager) logRecords(log logging.Logger, records []mvcc.Record) {
	if !log.Enabled(logging.LevelTrace) {
		return
	}

	for _, r := range records {
		value := decodeValueForLog(r.Value)
		log.Trace("Wrote record.", "rec_addr", r.Address, "rec_ts", r.Timestamp, "rec_value", value, "rec_flags", r.Flags)
	}
}
