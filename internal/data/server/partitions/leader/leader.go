package leader

import (
	"context"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/hlc"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
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

// TODO: request validation
// TODO: batch incoming txn
func (n *Leader) Autocommit(ctx context.Context, txn *data.Txn) (*data.TxnStatus, error) {
	cmd := &storage.TxnAutocommit{
		Timestamp: hlc.Now(),
		Txn:       txn,
	}

	return n.store.TxnAutocommit(cmd)
}

func (n *Leader) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	cmd := &storage.TxnPrepare{
		Timestamp: hlc.Now(),
		Txn:       req.Txn,
	}

	status, err := n.store.TxnPrepare(cmd)
	if err != nil {
		n.watchdog2pc.UpdateTxnStatus(status, req.Txn)
	}

	return status, err
}

func (n *Leader) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	cmd := &storage.TxnCommit{
		Timestamp: req.Timestamp,
		Id:        req.Id,
	}

	status, err := n.store.TxnCommit(cmd)
	if err != nil {
		n.watchdog2pc.UpdateTxnStatus(status, nil)
	}

	return status, err
}

func (n *Leader) Abort(ctx context.Context, req *data.AbortRequest) (*data.TxnStatus, error) {
	cmd := &storage.TxnAbort{
		Timestamp: hlc.Now(),
		Id:        req.Id,
	}

	status, err := n.store.TxnAbort(cmd)
	if err != nil {
		n.watchdog2pc.UpdateTxnStatus(status, nil)
	}

	return status, err
}

func (n *Leader) StoreDecision(ctx context.Context, dec *data.TxnDecision) (*data.TxnStatus, error) {
	if dec.Id.PrincipalPid != n.pid {
		n.log.Warn(ctx, "Received 2pc decision for another partition.", "principal_pid", dec.Id.PrincipalPid)
		return nil, errors.PermissionDenied
	}

	cmd := &storage.StoreTxnDecision{
		Timestamp: hlc.Now(),
		Decision:  dec,
	}

	status, err := n.store.StoreTxnDecision(cmd)
	if err != nil {
		n.watchdog2pc.UpdateTxnStatus(status, nil)
	}

	return status, err
}
