package txn

import (
	"fmt"

	"github.com/bcrusu/scout/internal/data"
)

// id is the map key friendly version of Id proto.
type id struct {
	PrincipalPid uint32
	ServerID     uint64
	Timestamp    uint64
}

func newId(i *data.TxnId) id {
	return id{
		PrincipalPid: i.PrincipalPid,
		ServerID:     i.ServerId,
		Timestamp:    i.Timestamp,
	}
}

type BatchResult struct {
	Status *data.TxnStatus
	Error  error
}

type BatchResults struct {
	Autocommit    []BatchResult
	Prepare       []BatchResult
	Commit        []BatchResult
	Abort         []BatchResult
	StoreDecision []BatchResult
	MarkTimedout  []BatchResult
}

type running struct {
	Id              id
	Timestamp       uint64
	State           data.TxnStatus_State
	ParticipantPids []uint32
	Decision        *data.Decision
}

func (t *id) ToProto() *data.TxnId {
	return &data.TxnId{
		PrincipalPid: t.PrincipalPid,
		ServerId:     t.ServerID,
		Timestamp:    t.Timestamp,
	}
}

func (i id) String() string {
	return fmt.Sprintf("principal_pid=%d server_id=%d timestamp=%d", i.PrincipalPid, i.ServerID, i.Timestamp)
}
