package txn

import (
	"slices"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

// Manager contains the read-write transaction management logic. The implementation
// needs to be fully deterministic to ensure identical state across all Raft replicas.
// Its state is advanced by the Raft FSM by calling the Apply method/s.
type Manager struct {
	pid          uint32
	db           *mvcc.DBBreaker
	cleanAfter   time.Duration
	log          logging.LoggerNoContext
	lock         sync.RWMutex // guards all below
	status       map[id]*Status
	prepared     map[id]*Prepared
	maxTimestamp uint64 // max HLC timestamp
}

func NewManager(pid uint32, db mvcc.DB) *Manager {
	// It is important that all replicas use the same config value to avoid
	// diverging states. Even so, transient diverging states are expected
	// when the value is changed. In the future could have the leader trigger
	// the periodic cleanup by writing a "cleanup" command to the Raft log.
	// For now, this approach works for handling low-impact cleanup logic.
	cleanAfter := config.Get().Transactions.CleanAfterReadWrite

	return &Manager{
		pid:        pid,
		db:         mvcc.NewDBBreaker(db),
		cleanAfter: cleanAfter,
		log:        logging.New("txn_manager").With("partition", pid).NoContext(),
		status:     map[id]*Status{},
		prepared:   map[id]*Prepared{},
	}
}

func (p *Manager) Snapshot() *Snapshot {
	p.lock.RLock()
	defer p.lock.RUnlock()

	prepared := utils.CloneProtoMapValues(p.prepared)
	for _, p := range prepared {
		p.Locks = nil // will be recreated from Prepared.Txn on restore
	}

	return &Snapshot{
		Status:       utils.MakeValueSlice(p.status),
		Prepared:     prepared,
		MaxTimestamp: p.maxTimestamp,
	}
}

func (p *Manager) Restore(snap *Snapshot) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.maxTimestamp = snap.MaxTimestamp

	p.status = make(map[id]*Status, len(snap.Status))
	for _, s := range snap.Status {
		p.status[s.Id.id()] = s
	}

	p.prepared = make(map[id]*Prepared, len(snap.Prepared))
	for _, x := range snap.Prepared {
		if !x.LocksReleased {
			x.Locks = x.Txn.BuildLocks()
		}

		p.prepared[x.Txn.Id.id()] = x
	}
}

func (p *Manager) getRunning() []running {
	p.lock.RLock()
	defer p.lock.RUnlock()

	result := make([]running, 0, len(p.prepared))

	for id, prepared := range p.prepared {
		if prepared.LocksReleased {
			continue
		}

		status := p.status[id]

		result = append(result, running{
			Id:              id,
			Timestamp:       status.Timestamp,
			State:           status.State,
			ParticipantPids: slices.Clone(status.ParticipantPids),
			Decision:        utils.CloneProto(prepared.Decision),
		})
	}

	return result
}

func (p *Manager) getLatestReadTimestamp(txn *Txn) uint64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.latestReadTimestampForLocks(txn.BuildLocks())
}

func (p *Manager) getPreparedTxn(id id, clone bool) *Txn {
	p.lock.RLock()
	defer p.lock.RUnlock()

	prepared, ok := p.prepared[id]
	if !ok {
		return nil
	}

	txn := prepared.Txn
	if clone {
		txn = utils.CloneProto(txn)
	}

	return txn
}
