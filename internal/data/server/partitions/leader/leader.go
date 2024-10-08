package leader

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ data.ServiceServer = (*Leader)(nil)
	_ utils.Lifecycle    = (*Leader)(nil)
)

// Leader implements the Leader role.
type Leader struct {
	data.UnsafeServiceServer
	pid         uint32
	log         logging.Logger
	store       storage.Store
	watchdog2pc *watchdog2PC
}

func New(pid uint32, store storage.Store, dataClient data.ServiceClient) *Leader {
	return &Leader{
		pid:         pid,
		log:         logging.WithComponent("leader").With("partition", pid),
		store:       store,
		watchdog2pc: newWatchdog2PC(pid, store, dataClient),
	}
}

func (n *Leader) Start(ctx context.Context) error {
	if err := n.watchdog2pc.Start(ctx); err != nil {
		return err
	}

	n.log.Debug(ctx, "Started leader")
	return nil
}

func (n *Leader) Stop() {
	n.watchdog2pc.Stop()
	n.log.NoContext().Debug("Stopped leader")
}

func (n *Leader) IsLeader() bool {
	return true
}

// TODO: request validation
func (n *Leader) Autocommit(ctx context.Context, txn *data.Txn) (*data.TxnStatus, error) {
	return n.store.Autocommit(txn)
}

func (n *Leader) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	status, err := n.store.Prepare(req.Txn)
	if err == nil {
		n.watchdog2pc.UpdateTxnStatus(status, req.Txn, nil)
	}

	return status, err
}

func (n *Leader) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	status, err := n.store.Commit(req.Id, req.CommitTimestamp)
	if err == nil {
		n.watchdog2pc.UpdateTxnStatus(status, nil, nil)
	}

	// TODO: cmd.FetchResults

	return status, err
}

func (n *Leader) Abort(ctx context.Context, req *data.AbortRequest) (*data.TxnStatus, error) {
	status, err := n.store.Abort(req.Id)
	if err == nil {
		n.watchdog2pc.UpdateTxnStatus(status, nil, nil)
	}

	return status, err
}

func (n *Leader) StoreDecision(ctx context.Context, dec *data.TxnDecision) (*data.TxnStatus, error) {
	if !dec.Commit {
		// only commit decisions are stored
		return nil, errors.InvalidRequest
	} else if dec.Id.PrincipalPid != n.pid {
		n.log.Warn(ctx, "Received 2pc decision for another partition.", "principal_pid", dec.Id.PrincipalPid)
		return nil, errors.PermissionDenied
	}

	status, err := n.store.StoreDecision(dec)
	if err == nil {
		n.watchdog2pc.UpdateTxnStatus(status, nil, dec)
	}

	return status, err
}
