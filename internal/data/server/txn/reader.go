package txn

import (
	"context"
	"sync"
	"time"

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
//     the latest possible commit timestamp computed as the min timstamp returned
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
	db           mvcc.DB
	log          logging.Logger
	cancelFunc   context.CancelFunc
	lock         sync.RWMutex
	prepared     map[id]*Txn
	preparedTime map[id]time.Time
}

func newReader(pid uint32, manager *Manager, db mvcc.DB) *reader {
	return &reader{
		config:       config.Get().Transactions,
		pid:          pid,
		manager:      manager,
		db:           db,
		log:          logging.WithComponent("txn").With("partition", pid),
		prepared:     map[id]*Txn{},
		preparedTime: map[id]time.Time{},
	}
}

func (p *reader) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(p.mainLoop)

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
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
		case <-ctx.Done():
			return
		}
	}
}

func (p *reader) AutocommitReadOnly(ctx context.Context, txn *Txn, timestamp uint64) (*Status, error) {
	latestTimestamp := p.manager.getLatestReadTimestamp(txn)

	if timestamp == 0 {
		timestamp = latestTimestamp
	} else if timestamp > latestTimestamp {
		// snapshot read trying to read past latest 'safe' timestamp.
		return nil, errors.FailedPrecondition
	}

	status := newEmptyStatus(txn, timestamp, Status_Committed)

	if err := p.fetchResults(ctx, txn, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *reader) AutocommitReadWrite(ctx context.Context, txn *Txn, status *Status) error {
	return p.fetchResults(ctx, txn, status)
}

func (p *reader) PrepareReadOnly(ctx context.Context, txn *Txn) (*Status, error) {
	id := txn.Id.id()
	timestamp := p.manager.getLatestReadTimestamp(txn)

	p.lock.Lock()
	p.prepared[id] = txn
	p.preparedTime[id] = time.Now()
	p.lock.Unlock()

	return newStatus(id, timestamp, Status_Prepared), nil
}

func (p *reader) CommitReadOnly(ctx context.Context, id *Id, timestamp uint64) (*Status, error) {
	p.lock.RLock()
	txn, ok := p.prepared[id.id()]
	p.lock.RUnlock()

	if !ok {
		return nil, errors.NotFound
	} else if timestamp > p.manager.getLatestReadTimestamp(txn) {
		return nil, errors.FailedPrecondition
	}

	status := newEmptyStatus(txn, timestamp, Status_Committed)

	if err := p.fetchResults(ctx, txn, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (p *reader) CommitReadWrite(ctx context.Context, status *Status) error {
	id := status.Id.id()
	txn := p.manager.getPreparedTxn(id, false)

	if txn == nil {
		// Txn should be returned by the manager unless CleanAfterReadWrite
		// config is set to a very low value.
		p.log.Error(ctx, "CommitReadWrite: expected prepared txn was not found.", "id", id)
		return errors.NotFound
	}

	return p.fetchResults(ctx, txn, status)
}

func (p *reader) fetchResults(ctx context.Context, txn *Txn, status *Status) error {
	// Point reads first, executed using a single Get/MultiGet call:
	ids, addrs := p.getPointReadAddrs(txn)

	records, err := p.db.Get(p.pid, status.Timestamp, addrs...)
	if err != nil {
		return err
	}

	for i, id := range ids {
		if record := records[i]; record == nil {
			status.ActionStatus[id] = newActionStatus(id, ActionStatus_KeyNotFound)
		} else if value, err := decodeValue(record.Value); err != nil {
			p.log.WithError(err).Error(ctx, "Failed to decode value.", "partition", p.pid, "address", record.Address, "timestamp", record.Timestamp)
			status.ActionStatus[id] = newActionStatus(id, ActionStatus_CorruptedData)
		} else {
			status.ActionStatus[id] = newActionStatus(id, ActionStatus_Success, value)
		}
	}

	// Range reads:
	for _, action := range txn.Actions {
		readRange, ok := action.Payload.(*Action_ReadRange)
		if !ok {
			continue
		}

		code, results, err := p.readRange(ctx, status.Timestamp, readRange.ReadRange)
		if err != nil {
			return err
		}

		status.ActionStatus[action.Id] = newActionStatus(action.Id, code, results...)
	}

	return nil
}

func (p *reader) getPointReadAddrs(txn *Txn) ([]uint32, []mvcc.Address) {
	var ids []uint32
	var addrs []mvcc.Address

	for _, action := range txn.Actions {
		switch x := action.Payload.(type) {
		case *Action_Read:
			ids = append(ids, action.Id)
			addrs = append(addrs, mvcc.NewAddress(x.Read.Keyspace, x.Read.Key))
		}
	}

	return ids, addrs
}

func (p *reader) readRange(ctx context.Context, timestamp uint64, rr *ReadRange) (ActionStatus_Code, []*Value, error) {
	start := mvcc.NewAddress(rr.Keyspace, rr.StartKey)
	end := mvcc.NewAddress(rr.Keyspace, rr.EndKey)

	iter, err := p.db.GetRange(p.pid, timestamp, start, end)
	if err != nil {
		return 0, nil, err
	}

	maxResults := min(int(rr.MaxResults), p.config.MaxIteratorResults)
	results := make([]*Value, 0, maxResults/10)

	for record, err := range iter {
		if err != nil {
			return 0, nil, errors.Wrap(err, "iteration failed")
		}

		value, err := decodeValue(record.Value)
		if err != nil {
			p.log.WithError(err).Error(ctx, "Failed to decode iterator value.", "partition", p.pid, "address", record.Address, "timestamp", record.Timestamp)

			if !p.config.SkipCorruptedData {
				return ActionStatus_CorruptedData, nil, nil
			}
			continue
		}

		results = append(results, value)

		if len(results) == maxResults {
			break
		}
	}

	return ActionStatus_Success, results, nil
}
