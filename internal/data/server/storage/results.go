package storage

import "github.com/bcrusu/graph/internal/data"

// TxnId is the map key friendly version of data.TxnId proto.
type TxnId struct {
	PrincipalPid uint32
	ServerID     uint64
	Timestamp    uint64
}

type TxnStatus struct {
	Status *data.TxnStatus
	Error  error
}

type TxnBatchResult struct {
	Autocommit    []TxnStatus
	Prepare       []TxnStatus
	Commit        []TxnStatus
	Abort         []TxnStatus
	StoreDecision []TxnStatus
}

func NewTxnId(id *data.TxnId) TxnId {
	return TxnId{
		PrincipalPid: id.PrincipalPid,
		ServerID:     id.ServerId,
		Timestamp:    id.Timestamp,
	}
}

func (t *TxnId) ToProto() *data.TxnId {
	return &data.TxnId{
		PrincipalPid: t.PrincipalPid,
		ServerId:     t.ServerID,
		Timestamp:    t.Timestamp,
	}
}
