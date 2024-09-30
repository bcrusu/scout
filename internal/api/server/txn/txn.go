package txn

import (
	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/hlc"
)

type Txn struct {
	processor          *Processor
	participantActions map[uint32][]*data.Action
	id                 *data.TxnId
	nextActionId       uint32
}

type TxnResult struct {
	Id           *data.TxnId
	Timestamp    uint64
	Success      bool
	ActionStatus map[uint32]*data.ActionStatus
}

func (t *Txn) Append(routingKey []byte, actions ...*data.Action) *Txn {
	pid := t.processor.partitioner.getPartition(routingKey)

	if t.id == nil {
		// first partition is selected as the principal partition
		t.id = &data.TxnId{
			PrincipalPid: pid,
			ServerId:     t.processor.identity.ServerID,
			Timestamp:    hlc.Now(),
		}
	}

	for _, action := range actions {
		action.Id = t.nextActionId
		t.participantActions[pid] = append(t.participantActions[pid], action)
		t.nextActionId++
	}

	return t
}

func (t *Txn) ParticipantCount() int {
	return len(t.participantActions)
}

func (t *TxnResult) GetError() error {
	var errs []error
	for _, r := range t.ActionStatus {
		if err := r.ToError(); err != nil {
			errs = append(errs, err)
		}
	}

	if !t.Success && len(errs) == 0 {
		return errors.Error("transaction failed")
	}
	return errors.Join(errs...)
}
