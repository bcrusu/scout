package txn

import "github.com/bcrusu/scout/internal/data"

func newStatus(id id, timestamp uint64, state data.TxnStatus_State) *data.TxnStatus {
	return &data.TxnStatus{
		Id:        id.ToProto(),
		Timestamp: timestamp,
		State:     state,
	}
}

func newFailedStatus(id id, timestamp uint64, actionId uint32, code data.ActionStatus_Code) *data.TxnStatus {
	return &data.TxnStatus{
		Id:        id.ToProto(),
		Timestamp: timestamp,
		State:     data.TxnStatus_Failed,
		ActionStatus: map[uint32]*data.ActionStatus{
			actionId: {
				Id:   actionId,
				Code: code,
			}},
	}
}

func newEmptyStatus(txn *data.Txn, timestamp uint64, state data.TxnStatus_State) *data.TxnStatus {
	return &data.TxnStatus{
		Id:           txn.Id,
		Timestamp:    timestamp,
		State:        state,
		ActionStatus: map[uint32]*data.ActionStatus{},
	}
}

func newActionStatus(id uint32, code data.ActionStatus_Code, results ...*data.Value) *data.ActionStatus {
	return &data.ActionStatus{
		Id:      id,
		Code:    code,
		Results: results,
	}
}
