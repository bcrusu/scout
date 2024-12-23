package txn

import (
	"context"
	"errors"
	"fmt"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/logging"
)

// id is the map key friendly version of data.TxnId proto.
type id struct {
	PrincipalPid uint32
	ServerID     uint64
	Timestamp    uint64
}

func (t *id) ToProto() *data.TxnId {
	return &data.TxnId{
		PrincipalPid: t.PrincipalPid,
		ServerId:     t.ServerID,
		Timestamp:    t.Timestamp,
	}
}

func (i id) String() string {
	return fmt.Sprintf("%d:%d:%d", i.PrincipalPid, i.ServerID, i.Timestamp)
}

func logErrors(ctx context.Context, msg string, id id, errs []error) {
	if !log.Enabled(logging.LevelDebug) {
		return
	}

	err := errors.Join(errs...)
	if err != nil {
		log.WithContext(ctx).WithError(err).Debug(msg, "id", id)
	}
}
