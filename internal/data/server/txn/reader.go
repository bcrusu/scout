package txn

import (
	"context"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ utils.Lifecycle = (*reader)(nil)
)

// The reader executes all read actions on the backing MVCC store and tracks the read-only
// transactions involving multiple partitions which are executed in two phases:
//   - a first prepare/negotiate phase where all partitions are queried to discover
//     the latest possible commit timestamp computed as the max timstamp returned
//     from all partitions. This commit timestamp ensures that all reads are performed
//     from a globally-consistent and causality-preserving snapshot.
//   - a second commit/fetch phase where results are fetched using the commit timestamp
//     from the first phase and is equivalent to a snapshot read at that timestamp.
//
// Prepared transactions are not persisted and in case of node failure clients are
// expected to retry the operation.
//
// Snapshot read transactions are not allowed to read past the latest 'safe' timestamp.
type reader struct {
	config       config.Transactions
	pid          uint32
	manager      *Manager
	writer       *writer
	db           mvcc.DB
	meters       readerMeters
	log          logging.Logger
	cancelFunc   context.CancelFunc
	lock         sync.RWMutex
	prepared     map[id]*data.Txn
	preparedTime map[id]time.Time
}

func newReader(pid uint32, manager *Manager, writer *writer, db mvcc.DB) *reader {
	return &reader{
		config:       config.Get().Transactions,
		pid:          pid,
		manager:      manager,
		writer:       writer,
		db:           db,
		meters:       newReaderMeters(pid),
		log:          logging.New("txn").With("partition", pid),
		prepared:     map[id]*data.Txn{},
		preparedTime: map[id]time.Time{},
	}
}

func (p *reader) Start(ctx context.Context) error {
	p.cancelFunc = utils.RunAsync(ctx, p.mainLoop)
	return nil
}

func (p *reader) Stop() {
	p.cancelFunc()
}

func (s *reader) mainLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.CleanAfterReadOnly / 4)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			oldest := time.Now().Add(-s.config.CleanAfterReadOnly)
			toRemove := map[id]bool{}

			s.lock.RLock()

			for id, preparedTime := range s.preparedTime {
				if preparedTime.Before(oldest) {
					toRemove[id] = true
				}
			}

			s.lock.RUnlock()
			s.lock.Lock()

			for id := range toRemove {
				delete(s.prepared, id)
				delete(s.preparedTime, id)
			}

			s.lock.Unlock()

			s.meters.Prepared.Add(-len(toRemove))
		case <-ctx.Done():
			return
		}
	}
}

func (p *reader) AutocommitSnapshotRead(ctx context.Context, txn *data.Txn, timestamp uint64) (*data.TxnStatus, error) {
	log := p.log.WithContext(ctx).With("cmd", "AutocommitSnapshotRead", "id", txn.Id.LogString(), "req_ts", timestamp)

	if err := p.checkSafeTimestamp(log, txn, timestamp); err != nil {
		return nil, err
	}

	status := newEmptyStatus(txn, timestamp, data.TxnStatus_Committed)

	if err := p.fetchResults(log, txn, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *reader) AutocommitReadOnly(ctx context.Context, txn *data.Txn) (*data.TxnStatus, error) {
	safeTimestamp, _ := p.manager.getSafeTimestamp(txn)
	log := p.log.WithContext(ctx).With("cmd", "AutocommitReadOnly", "id", txn.Id.LogString(), "safe_ts", safeTimestamp)

	status := newEmptyStatus(txn, safeTimestamp, data.TxnStatus_Committed)

	if err := p.fetchResults(log, txn, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *reader) AutocommitReadWrite(ctx context.Context, txn *data.Txn, status *data.TxnStatus) error {
	log := p.log.WithContext(ctx).With("cmd", "AutocommitReadWrite", "id", txn.Id.LogString())
	return p.fetchResults(log, txn, status)
}

func (p *reader) PrepareReadOnly(ctx context.Context, txn *data.Txn) (*data.TxnStatus, error) {
	id := newId(txn.Id)
	safeTimestamp, _ := p.manager.getSafeTimestamp(txn)

	log := p.log.WithContext(ctx).With("cmd", "PrepareReadOnly", "id", id, "safe_ts", safeTimestamp)

	p.lock.Lock()
	p.prepared[id] = txn
	p.preparedTime[id] = time.Now()
	p.lock.Unlock()

	p.meters.Prepared.Add(1)
	log.Trace("Prepared.")
	return newStatus(id, safeTimestamp, data.TxnStatus_Prepared), nil
}

func (p *reader) CommitReadOnly(ctx context.Context, txnId *data.TxnId, timestamp uint64) (*data.TxnStatus, error) {
	id := newId(txnId)
	log := p.log.WithContext(ctx).With("cmd", "CommitReadOnly", "id", id, "req_ts", timestamp)

	p.lock.RLock()
	txn, ok := p.prepared[id]
	p.lock.RUnlock()

	if !ok {
		log.Error("Prepared not found.")
		return nil, errors.NotFound
	}

	if err := p.checkSafeTimestamp(log, txn, timestamp); err != nil {
		return nil, err
	}

	status := newEmptyStatus(txn, timestamp, data.TxnStatus_Committed)

	if err := p.fetchResults(log, txn, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *reader) CommitReadWrite(ctx context.Context, status *data.TxnStatus) error {
	id := newId(status.Id)
	log := p.log.WithContext(ctx).With("cmd", "CommitReadWrite", "id", id)
	txn := p.manager.getPreparedTxn(id, false)

	if txn == nil {
		// Txn should be returned by the manager unless CleanAfterReadWrite
		// config is set to a very low value.
		log.Error("Prepared not found.")
		return errors.NotFound
	}

	return p.fetchResults(log, txn, status)
}

func (p *reader) fetchResults(log logging.Logger, txn *data.Txn, status *data.TxnStatus) error {
	// Point reads first, executed using a single Get/MultiGet call:
	ids, addrs := p.getPointReadAddrs(txn)

	records, err := p.db.Get(p.pid, status.Timestamp, addrs...)
	if err != nil {
		return err
	}

	p.logRecords(log, addrs, records)

	for i, id := range ids {
		if record := records[i]; record == nil {
			status.ActionStatus[id] = newActionStatus(id, data.ActionStatus_KeyNotFound)
		} else if value, err := decodeValue(record.Value); err != nil {
			log.WithError(err).Error("Failed to decode value.", "rec_addr", record.Address, "rec_ts", record.Timestamp)
			status.ActionStatus[id] = newActionStatus(id, data.ActionStatus_CorruptedData)
		} else {
			status.ActionStatus[id] = newActionStatus(id, data.ActionStatus_Success, value)
		}
	}

	// Range reads:
	for _, action := range txn.Actions {
		readRange, ok := action.Payload.(*data.Action_ReadRange)
		if !ok {
			continue
		}

		code, results, err := p.readRange(log, status.Timestamp, readRange.ReadRange)
		if err != nil {
			return err
		}

		status.ActionStatus[action.Id] = newActionStatus(action.Id, code, results...)
	}

	return nil
}

func (p *reader) getPointReadAddrs(txn *data.Txn) ([]uint32, []mvcc.Address) {
	var ids []uint32
	var addrs []mvcc.Address

	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *data.Action_Read:
			ids = append(ids, action.Id)
			addrs = append(addrs, mvcc.NewAddress(x.Read.Keyspace, x.Read.Key))
		}
	}

	return ids, addrs
}

func (p *reader) readRange(log logging.Logger, timestamp uint64, rr *data.ReadRange) (data.ActionStatus_Code, []*data.Value, error) {
	start := mvcc.NewAddress(rr.Keyspace, rr.StartKey)
	end := mvcc.NewAddress(rr.Keyspace, rr.EndKey)

	iter, err := p.db.GetRange(p.pid, timestamp, start, end)
	if err != nil {
		return 0, nil, err
	}

	maxResults := min(int(rr.MaxResults), p.config.MaxIteratorResults)
	results := make([]*data.Value, 0, maxResults/10)

	for record, err := range iter {
		if err != nil {
			return 0, nil, errors.Wrap(err, "iteration failed")
		}

		value, err := decodeValue(record.Value)
		if err != nil {
			log.WithError(err).Error("Failed to decode iterator value.", "rec_addr", record.Address, "rec_ts", record.Timestamp)

			if !p.config.SkipCorruptedData {
				return data.ActionStatus_CorruptedData, nil, nil
			}
			continue
		}

		results = append(results, value)

		if len(results) == maxResults {
			break
		}
	}

	return data.ActionStatus_Success, results, nil
}

func (p *reader) logRecords(log logging.Logger, addrs []mvcc.Address, records []*mvcc.Record) {
	if !log.Enabled(logging.LevelTrace) {
		return
	}

	for i, addr := range addrs {
		r := records[i]

		if r == nil {
			log.Trace("Read record not found.", "addr", addr)
			continue
		}

		value := decodeValueForLog(r.Value)
		log.Trace("Read record.", "addr", addr, "rec_addr", r.Address, "rec_ts", r.Timestamp, "rec_value", value, "rec_flags", r.Flags)
	}
}

func (p *reader) checkSafeTimestamp(log logging.Logger, txn *data.Txn, timestamp uint64) error {
	safeTimestamp, conflicting := p.manager.getSafeTimestamp(txn)
	if timestamp <= safeTimestamp {
		return nil
	}

	// If there are conflicting txns just return early and let the caller retry.
	// As a future optimization, could also try sleeping for a configured duration
	// and check again the safe ts in the hope that the conflicting txns completed,
	// but it needs more thought to decide the cases where sleep is beneficial
	// compared to caller retry. Also, it only makes sense for the leader to update
	// the timestamp (i.e. when writer is not nil).
	if conflicting || p.writer == nil {
		log.Trace("Tried to read above safe timestamp.", "safe_ts", safeTimestamp)
		return errors.TimeOutOfRange
	}

	// if there are no conflicting pending txns, bump max timestamp...
	if err := p.writer.UpdateTimestamp(timestamp); err != nil {
		return err
	}

	// ...and check again
	safeTimestamp, _ = p.manager.getSafeTimestamp(txn)
	if timestamp <= safeTimestamp {
		return nil
	}

	log.Trace("Tried to read above safe timestamp.", "safe_ts", safeTimestamp)
	return errors.TimeOutOfRange
}
