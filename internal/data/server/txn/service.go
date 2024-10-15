package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
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
}

type RaftStore interface {
	ApplyBatch(batch *Batch) (<-chan multiraft.AsyncResult, error)
}

func NewService(pid uint32, raftStore RaftStore, db kv.DB, manager *Manager, client TxnServiceClient) *Service {
	s := &Service{
		config:  config.Get().Transactions,
		pid:     pid,
		batcher: newBatcher(raftStore),
		reader:  newReader(pid, db),
		manager: manager,
		log:     logging.WithComponent("txn").With("partition", pid),
	}

	if client != nil {
		s.watchdog2PC = newWatchdog2PC(pid, s, manager, client)
	}

	return s
}

func NewServiceNoWatchdog(pid uint32, raftStore RaftStore, db kv.DB, manager *Manager) *Service {
	return NewService(pid, raftStore, db, manager, nil)
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.batcher.Start(ctx); err != nil {
		return err
	}

	if s.watchdog2PC != nil {
		if err := s.watchdog2PC.Start(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) Stop() {
	s.batcher.Stop()

	if s.watchdog2PC != nil {
		s.watchdog2PC.Stop()
	}
}

func (n *Service) Autocommit(ctx context.Context, req *AutocommitRequest) (*Status, error) {
	if req.PartitionId != n.pid {
		return nil, errors.InvalidRequest
	} else if req.Txn.IsReadOnly() {
		return n.reader.Read(req.Txn, req.ReadTimestamp)
	}

	return n.batcher.Apply(&Autocommit{Txn: req.Txn, Timestamp: hlc.Now()})
}

func (n *Service) Prepare(ctx context.Context, req *PrepareRequest) (*Status, error) {
	if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	} else if req.ReadOnly {
		return n.reader.PrepareReadOnly(req.Txn)
	}

	status, err := n.batcher.Apply(&Prepare{Txn: req.Txn, Timestamp: hlc.Now()})
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, req.Txn, nil)
	}

	return status, nil
}

func (n *Service) Commit(ctx context.Context, req *CommitRequest) (*Status, error) {
	if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	} else if err := hlc.Update(req.CommitTimestamp); err != nil {
		return nil, err
	} else if req.ReadOnly {
		return n.reader.ReadPreparedReadOnly(req.Id, req.CommitTimestamp)
	}

	// the commit timestamp is decided by txn participants
	status, err := n.batcher.Apply(&Commit{Id: req.Id, Timestamp: req.CommitTimestamp})
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, nil)
	}

	if !req.FetchResults {
		return status, nil
	}

	if err := n.reader.ReadResults(status); err != nil {
		return nil, err
	}

	return status, nil
}

func (n *Service) Abort(ctx context.Context, req *AbortRequest) (*Status, error) {
	if req.ParticipantPid != n.pid {
		return nil, errors.InvalidRequest
	}

	status, err := n.batcher.Apply(&Abort{Id: req.Id, Timestamp: hlc.Now()})
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, nil)
	}

	return status, nil
}

func (n *Service) StoreDecision(ctx context.Context, dec *Decision) (*Status, error) {
	if dec.Id.PrincipalPid != n.pid {
		return nil, errors.PermissionDenied
	} else if !dec.Commit {
		// only commit decisions are stored
		return nil, errors.InvalidRequest
	}

	status, err := n.batcher.Apply(&StoreDecision{Decision: dec, Timestamp: hlc.Now()})
	if err != nil {
		return nil, err
	} else if n.watchdog2PC != nil {
		n.watchdog2PC.UpdateStatus(status, nil, dec)
	}

	return status, nil
}

func (n *Service) markTimedout(id *Id, releaseLocks bool) (*Status, error) {
	return n.batcher.Apply(&MarkTimedout{Id: id, Timestamp: hlc.Now(), ReleaseLocks: releaseLocks})
}
