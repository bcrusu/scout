package txn

import "github.com/bcrusu/graph/internal/data"

type Multi struct {
	id          *data.TxnId
	partitioner *partitioner
	byPartition map[uint32]*data.Txn
}

func (t *Multi) getTxn(partitionID uint32) *data.Txn {
	result, ok := t.byPartition[partitionID]
	if !ok {
		result = &data.Txn{
			Id: t.id,
		}
		t.byPartition[partitionID] = result
	}
	return result
}

func (t *Multi) Append(routingKey []byte, actions ...*data.Action) *Multi {
	partitionID := t.partitioner.getPartition(routingKey)
	txn := t.getTxn(partitionID)
	txn.Actions = append(txn.Actions, actions...)
	return t
}
