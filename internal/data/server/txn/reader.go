package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
)

type reader struct {
	config config.Transactions
	pid    uint32
	db     mvcc.DB
	log    logging.Logger
}

// TODO
func newReader(pid uint32, db mvcc.DB) *reader {
	return &reader{
		config: config.Get().Transactions,
		pid:    pid,
		db:     db,
		log:    logging.WithComponent("txn").With("partition", pid),
	}
}

func (p *reader) Read(ctx context.Context, txn *Txn, timestamp uint64) (*Status, error) {
	return p.read(ctx, txn, timestamp)
}

func (p *reader) ReadResults(status *Status) error {
	return nil
}

func (p *reader) PrepareReadOnly(txn *Txn) (*Status, error) {
	return nil, nil
}

func (p *reader) ReadPreparedReadOnly(id *Id, timestamp uint64) (*Status, error) {
	return nil, nil
}

func (p *reader) read(ctx context.Context, txn *Txn, timestamp uint64) (*Status, error) {
	actionStatus := map[uint32]*ActionStatus{}

	// Point reads
	ids, addrs := p.getPointReadAddrs(txn)

	records, err := p.db.Get(p.pid, timestamp, addrs...)
	if err != nil {
		return nil, err
	}

	for i, id := range ids {
		record := records[i]

		code := ActionStatus_KeyNotFound
		var results []*Value

		if record != nil {
			if x, err := decodeValue(record.Value); err != nil {
				p.log.WithError(err).Error(ctx, "Failed to decode value.", "partition", p.pid, "address", record.Address, "timestamp", record.Timestamp)
				code = ActionStatus_CorruptedData
			} else {
				code = ActionStatus_Success
				results = []*Value{x}
			}
		}

		actionStatus[id] = &ActionStatus{
			Id:      id,
			Code:    code,
			Results: results,
		}
	}

	// Range reads
	for _, action := range txn.Actions {
		readRange, ok := action.Payload.(*Action_ReadRange)
		if !ok {
			continue
		}

		code, results, err := p.readRange(ctx, readRange.ReadRange, timestamp)
		if err != nil {
			return nil, err
		}

		actionStatus[action.Id] = &ActionStatus{
			Id:      action.Id,
			Code:    code,
			Results: results,
		}
	}

	return &Status{
		Id:           txn.Id,
		Timestamp:    timestamp,
		State:        State_Committed,
		ActionStatus: actionStatus,
	}, nil
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

func (p *reader) readRange(ctx context.Context, rr *ReadRange, timestamp uint64) (ActionStatus_Code, []*Value, error) {
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
