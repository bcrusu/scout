package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/tracing"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ utils.Lifecycle = (*Service)(nil)
)

type Service struct {
	config      config.Transactions
	pid         uint32
	isLeader    bool
	writer      *writer
	reader      *reader
	manager     *Manager
	watchdog2PC *watchdog2PC
	log         logging.Logger
	components  []utils.Lifecycle
}

type RaftStore interface {
	ApplyBatch(batch *data.TxnBatch) <-chan multiraft.AsyncResult
}

func NewServiceLeader(pid uint32, manager *Manager, db mvcc.DB, raftStore RaftStore, client data.ServiceClient) *Service {
	log := logging.New("txn").With("pid", pid)
	writer := newWriter(raftStore, log)
	reader := newReader(pid, manager, writer, db)
	watchdog2PC := newWatchdog2PC(pid, writer, manager, client)

	return &Service{
		config:      config.Get().Transactions,
		pid:         pid,
		isLeader:    true,
		writer:      writer,
		reader:      reader,
		manager:     manager,
		watchdog2PC: watchdog2PC,
		log:         log,
		components:  []utils.Lifecycle{writer, reader, watchdog2PC},
	}
}

func NewServiceFollower(pid uint32, manager *Manager, db mvcc.DB) *Service {
	reader := newReader(pid, manager, nil, db)

	return &Service{
		config:     config.Get().Transactions,
		pid:        pid,
		isLeader:   false,
		reader:     reader,
		manager:    manager,
		log:        logging.New("txn").With("pid", pid),
		components: []utils.Lifecycle{reader},
	}
}

func (n *Service) Start(ctx context.Context) error {
	if err := utils.LifecycleStart(ctx, n.log, n.components...); err != nil {
		return err
	} else if !n.isLeader {
		return nil
	}

	if err := n.startLeader(ctx); err != nil {
		return errors.Wrap(err, "failed to start leader")
	}

	return nil
}

func (n *Service) Stop() {
	utils.LifecycleStop(n.log, n.components...)
}

func (n *Service) Autocommit(ctx context.Context, req *data.AutocommitRequest) (*data.TxnStatus, error) {
	if req.PartitionId != n.pid {
		return nil, errors.InvalidRequest
	} else if req.IsSnapshotRead() {
		if err := hlc.Update(req.ReadTimestamp); err != nil {
			return nil, err
		}
		return n.reader.AutocommitSnapshotRead(ctx, req.Txn, req.ReadTimestamp)
	} else if !n.isLeader {
		return nil, errors.NotLeader
	} else if req.Txn.IsReadOnly() {
		return n.reader.AutocommitReadOnly(ctx, req.Txn)
	}

	cmd := &data.Autocommit{
		Txn:   req.Txn,
		Trace: tracing.GetTraceID(ctx),
	}

	status, err := n.writer.Apply(cmd)
	if err != nil {
		return nil, err
	} else if err := n.reader.AutocommitReadWrite(ctx, req.Txn, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (n *Service) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	if !n.isLeader {
		return nil, errors.NotLeader
	} else if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	} else if req.ReadOnly {
		return n.reader.PrepareReadOnly(ctx, req.Txn)
	}

	cmd := &data.Prepare{
		Txn:   req.Txn,
		Trace: tracing.GetTraceID(ctx),
	}

	status, err := n.writer.Apply(cmd)
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, req.Txn, nil)
	}

	return status, nil
}

func (n *Service) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	if !n.isLeader {
		return nil, errors.NotLeader
	} else if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	} else if err := hlc.Update(req.CommitTimestamp); err != nil {
		return nil, err
	} else if req.ReadOnly {
		return n.reader.CommitReadOnly(ctx, req.Id, req.CommitTimestamp)
	}

	cmd := &data.Commit{
		Id:        req.Id,
		Timestamp: req.CommitTimestamp,
		Trace:     tracing.GetTraceID(ctx),
	}

	// the commit timestamp is decided by txn participants
	status, err := n.writer.Apply(cmd)
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, nil)
	}

	if !req.FetchResults {
		return status, nil
	} else if err := n.reader.CommitReadWrite(ctx, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (n *Service) Abort(ctx context.Context, req *data.AbortRequest) (*data.TxnStatus, error) {
	if !n.isLeader {
		return nil, errors.NotLeader
	} else if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	}

	cmd := &data.Abort{
		Id:    req.Id,
		Trace: tracing.GetTraceID(ctx),
	}

	status, err := n.writer.Apply(cmd)
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, nil)
	}

	return status, nil
}

func (n *Service) StoreDecision(ctx context.Context, dec *data.Decision) (*data.TxnStatus, error) {
	if !n.isLeader {
		return nil, errors.NotLeader
	} else if dec.Id.PrincipalPid != n.pid {
		return nil, errors.PermissionDenied
	} else if !dec.Commit {
		// only commit decisions are stored
		return nil, errors.InvalidRequest
	} else if err := hlc.Update(dec.CommitTimestamp); err != nil {
		return nil, err
	}

	cmd := &data.StoreDecision{
		Decision: dec,
		Trace:    tracing.GetTraceID(ctx),
	}

	status, err := n.writer.Apply(cmd)
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, dec)
	}

	return status, nil
}

func (n *Service) startLeader(ctx context.Context) error {
	maxTimestamp := n.manager.getMaxTimestamp()

	// if the store is empty, perform the first write:
	if maxTimestamp == 0 {
		if err := n.writer.UpdateTimestamp(hlc.Now()); err != nil {
			return errors.Wrap(err, "failed to set the initial timestamp")
		}
	}

	return n.updateHlc(ctx, maxTimestamp)
}

func (n *Service) updateHlc(ctx context.Context, timestamp uint64) error {
	err := hlc.Update(timestamp)
	if err == nil {
		return nil
	}

	n.log.WithError(err).Error("Failed to update HLC. Sleeping...")

	if err := hlc.Sleep(ctx, timestamp); err != nil {
		return errors.Wrapf(err, "sleep interrupted during HLC update")
	}

	return nil
}
