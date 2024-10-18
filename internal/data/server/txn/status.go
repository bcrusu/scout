package txn

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

func newEmptyStatus(txn *Txn, timestamp uint64, state Status_State) *Status {
	return &Status{
		Id:           txn.Id,
		Timestamp:    timestamp,
		State:        state,
		ActionStatus: map[uint32]*ActionStatus{},
	}
}

func newActionStatus(id uint32, code ActionStatus_Code, results ...*Value) *ActionStatus {
	return &ActionStatus{
		Id:      id,
		Code:    code,
		Results: results,
	}
}
