package txn

import (
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
)

type reader struct {
	partitionID uint32
	db          *mvcc.DBBreaker
}

func newReader(partitionID uint32, db kv.DB) *reader {
	return &reader{
		partitionID: partitionID,
		db:          mvcc.NewDBBreaker(mvcc.New(partitionID, db)),
	}
}

// TODO
func (p *reader) Read(txn *Txn, readTimestamp uint64) (*Status, error) {
	return nil, nil
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
