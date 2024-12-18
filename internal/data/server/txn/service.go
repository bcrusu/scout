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
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ utils.Lifecycle = (*Service)(nil)
)

type Service struct {
	config      config.Transactions
	pid         uint32
	batcher     *batcher
	reader      *reader
	manager     *Manager
	watchdog2PC *watchdog2PC
	log         logging.Logger
	components  []utils.Lifecycle
}

type RaftStore interface {
	ApplyBatch(batch *data.TxnBatch) <-chan multiraft.AsyncResult
}

func NewService(pid uint32, raftStore RaftStore, manager *Manager, db mvcc.DB, client data.ServiceClient) *Service {
	s := &Service{
		config:  config.Get().Transactions,
		pid:     pid,
		batcher: newBatcher(raftStore),
		reader:  newReader(pid, manager, db),
		manager: manager,
		log:     logging.New("txn").With("partition", pid),
	}

	s.components = []utils.Lifecycle{
		s.batcher,
		s.reader,
	}

	if client != nil {
		s.watchdog2PC = newWatchdog2PC(pid, s, manager, client)
		s.components = append(s.components, s.watchdog2PC)
	}

	return s
}

func NewServiceNoWatchdog(pid uint32, raftStore RaftStore, manager *Manager, db mvcc.DB) *Service {
	return NewService(pid, raftStore, manager, db, nil)
}

func (s *Service) Start(ctx context.Context) error {
	return utils.LifecycleStart(ctx, s.log, s.components...)
}

func (s *Service) Stop() {
	utils.LifecycleStop(s.log, s.components...)
}

func (n *Service) Autocommit(ctx context.Context, req *data.AutocommitRequest) (*data.TxnStatus, error) {
	if req.PartitionId != n.pid {
		return nil, errors.InvalidRequest
	} else if req.Txn.IsReadOnly() {
		return n.reader.AutocommitReadOnly(ctx, req.Txn, req.ReadTimestamp)
	}

	status, err := n.batcher.Apply(&data.Autocommit{Txn: req.Txn})
	if err != nil {
		return nil, err
	} else if err := n.reader.AutocommitReadWrite(ctx, req.Txn, status); err != nil {
		return nil, err
	}

	return status, nil
}

func (n *Service) Prepare(ctx context.Context, req *data.PrepareRequest) (*data.TxnStatus, error) {
	if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	} else if req.ReadOnly {
		return n.reader.PrepareReadOnly(ctx, req.Txn)
	}

	status, err := n.batcher.Apply(&data.Prepare{Txn: req.Txn})
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, req.Txn, nil)
	}

	return status, nil
}

func (n *Service) Commit(ctx context.Context, req *data.CommitRequest) (*data.TxnStatus, error) {
	if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	} else if err := hlc.Update(req.CommitTimestamp); err != nil {
		return nil, err
	} else if req.ReadOnly {
		return n.reader.CommitReadOnly(ctx, req.Id, req.CommitTimestamp)
	}

	// the commit timestamp is decided by txn participants
	status, err := n.batcher.Apply(&data.Commit{Id: req.Id, Timestamp: req.CommitTimestamp})
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
	if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	}

	status, err := n.batcher.Apply(&data.Abort{Id: req.Id})
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, nil)
	}

	return status, nil
}

func (n *Service) StoreDecision(ctx context.Context, dec *data.Decision) (*data.TxnStatus, error) {
	if dec.Id.PrincipalPid != n.pid {
		return nil, errors.PermissionDenied
	} else if !dec.Commit {
		// only commit decisions are stored
		return nil, errors.InvalidRequest
	}

	status, err := n.batcher.Apply(&data.StoreDecision{Decision: dec})
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, dec)
	}

	return status, nil
}

func (n *Service) markTimedout(id *data.TxnId, releaseLocks bool) (*data.TxnStatus, error) {
	return n.batcher.Apply(&data.MarkTimedout{Id: id, ReleaseLocks: releaseLocks})
}
