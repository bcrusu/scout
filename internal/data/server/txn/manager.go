package txn

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ utils.Lifecycle = (*Manager)(nil)
)

// Manager contains the read-write transaction management logic. The implementation
// needs to be fully deterministic to ensure identical state across all Raft replicas.
// Its state is advanced by the Raft FSM by calling the Apply method/s.
type Manager struct {
	pid          uint32
	db           *mvcc.DBBreaker
	cleanAfter   time.Duration
	meters       managerMeters
	log          logging.Logger
	lock         sync.RWMutex // guards all below
	status       map[id]*data.TxnStatus
	prepared     map[id]*data.Prepared
	maxTimestamp uint64 // max observed HLC timestamp
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
		meters:     newManagerMeters(pid),
		log:        logging.New("txn").With("pid", pid),
		status:     map[id]*data.TxnStatus{},
		prepared:   map[id]*data.Prepared{},
	}
}

func (p *Manager) Start(ctx context.Context) error {
	return nil
}

func (p *Manager) Stop() {
	p.meters.Tracked.Add(-len(p.status))
	p.meters.Running.Add(-p.countRunning())
	p.meters.LocksHeld.Add(-p.countLocksHeld())
}

func (p *Manager) Snapshot() *data.TxnSnapshot {
	p.lock.RLock()
	defer p.lock.RUnlock()

	prepared := utils.CloneProtoMapValues(p.prepared)
	for _, p := range prepared {
		p.Locks = nil // will be recreated from Prepared.Txn on restore
	}

	return &data.TxnSnapshot{
		Status:       utils.MakeValueSlice(p.status),
		Prepared:     prepared,
		MaxTimestamp: p.maxTimestamp,
	}
}

func (p *Manager) Restore(snap *data.TxnSnapshot) {
	p.lock.Lock()
	defer p.lock.Unlock()

	oldTracked := len(p.status)
	oldRunning := p.countRunning()
	oldLocksHeld := p.countLocksHeld()

	p.maxTimestamp = snap.MaxTimestamp

	p.status = make(map[id]*data.TxnStatus, len(snap.Status))
	for _, s := range snap.Status {
		p.status[newId(s.Id)] = s
	}

	p.prepared = make(map[id]*data.Prepared, len(snap.Prepared))
	for _, x := range snap.Prepared {
		if !x.LocksReleased {
			x.Locks = x.Txn.BuildLocks()
		}

		p.prepared[newId(x.Txn.Id)] = x
	}

	p.meters.Tracked.Add(oldTracked - len(p.status))
	p.meters.Running.Add(oldRunning - p.countRunning())
	p.meters.LocksHeld.Add(oldLocksHeld - p.countLocksHeld())
}

func (p *Manager) countRunning() int {
	count := 0
	for _, prepared := range p.prepared {
		if !prepared.LocksReleased {
			count++
		}
	}
	return count
}

func (p *Manager) getRunning() []running {
	p.lock.RLock()
	defer p.lock.RUnlock()

	result := make([]running, 0, len(p.prepared))

	for id, prepared := range p.prepared {
		if id.PrincipalPid != p.pid || prepared.LocksReleased {
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

func (p *Manager) getMaxTimestamp() uint64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.maxTimestamp
}

// Returns the max timestamp the txn can read and if there are
// conflicting locks held by other prepared txns.
func (p *Manager) getSafeTimestamp(txn *data.Txn) (uint64, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.safeTimestampForLocks(txn.BuildLocks())
}

func (p *Manager) getPreparedTxn(id id, clone bool) *data.Txn {
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
