package txn

import (
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Txn struct {
	processor          *Processor
	participantActions map[uint32][]*data.Action
	id                 *data.TxnId
	nextActionId       uint32
	readTimestamp      uint64
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

func (t *Txn) SnapshotReadAt(time time.Time) *Txn {
	if !time.IsZero() {
		t.readTimestamp = hlc.FromTime(time)
	}
	return t
}

func (t *Txn) SnapshotReadAtTimestamp(ts *timestamppb.Timestamp) *Txn {
	if ts != nil {
		t.readTimestamp = hlc.FromTimestamp(ts)
	}
	return t
}

func (t *Txn) ParticipantCount() int {
	return len(t.participantActions)
}

func (t *Txn) IsReadOnly() bool {
	for _, actions := range t.participantActions {
		for _, a := range actions {
			if !a.IsReadOnly() {
				return false
			}
		}
	}
	return true
}

func (t *TxnResult) GetError() error {
	var errs []error
	for _, r := range t.ActionStatus {
		if err := r.ToError(); err != nil {
			errs = append(errs, err)
		}
	}

	// sanity check:
	if !t.Success && len(errs) == 0 {
		log.Warn("TxnResult.Success is false without any failed action.", "txn", t.Id)
		return errors.Error("transaction failed")
	}

	return errors.Join(errs...)
}

func (t *TxnResult) GetFirstError() error {
	for _, r := range t.ActionStatus {
		if err := r.ToError(); err != nil {
			return err
		}
	}

	// sanity check:
	if !t.Success {
		log.Warn("TxnResult.Success is false without any failed action.", "txn", t.Id)
		return errors.Error("transaction failed")
	}

	return nil
}
