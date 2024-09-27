package storage

import "github.com/bcrusu/graph/internal/data"

// TxnId is the map key friendly version of data.TxnId proto.
type TxnId struct {
	ServerID  uint64
	Timestamp uint64
}

type TxnBatchResult struct {
	Status []*data.TxnStatus
}

func newTxnId(id *data.TxnId) TxnId {
	return TxnId{
		ServerID:  id.ServerId,
		Timestamp: id.Timestamp,
	}
}

func (t *TxnId) Proto() *data.TxnId {
	return &data.TxnId{
		ServerId:  t.ServerID,
		Timestamp: t.Timestamp,
	}
}
