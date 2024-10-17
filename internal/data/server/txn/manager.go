package txn

import (
	"slices"
	"sync"

	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/utils"
)

// TODO: prune status for old txn
type Manager struct {
	pid          uint32
	db           *mvcc.DBBreaker
	lock         sync.RWMutex // guards all below
	status       map[id]*Status
	prepared     map[id]*prepared
	decisions    map[id]*Decision
	maxTimestamp uint64 // max HLC timestamp
}

type prepared struct {
	Txn   *Txn
	Locks []*Lock
}

func NewManager(pid uint32, db mvcc.DB) *Manager {
	return &Manager{
		pid:       pid,
		db:        mvcc.NewDBBreaker(db),
		status:    map[id]*Status{},
		prepared:  map[id]*prepared{},
		decisions: map[id]*Decision{},
	}
}

func (p *Manager) Snapshot() *Snapshot {
	p.lock.RLock()
	defer p.lock.RUnlock()

	prepared := make([]*Txn, 0, len(p.prepared))
	for _, p := range p.prepared {
		prepared = append(prepared, p.Txn)
	}

	return &Snapshot{
		Status:       utils.MakeValueSlice(p.status),
		Prepared:     prepared,
		Decisions:    utils.MakeValueSlice(p.decisions),
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

	p.prepared = make(map[id]*prepared, len(snap.Prepared))
	for _, txn := range snap.Prepared {
		p.prepared[txn.Id.id()] = &prepared{
			Txn:   txn,
			Locks: txn.BuildLocks(),
		}
	}

	p.decisions = make(map[id]*Decision, len(snap.Decisions))
	for _, d := range snap.Decisions {
		p.decisions[d.Id.id()] = d
	}
}

func (p *Manager) getRunning() []running {
	p.lock.RLock()
	defer p.lock.RUnlock()

	result := make([]running, 0, len(p.prepared))

	for id := range p.prepared {
		status := p.status[id]

		result = append(result, running{
			Id:              id,
			Timestamp:       status.Timestamp,
			State:           status.State,
			ParticipantPids: slices.Clone(status.ParticipantPids),
			Decision:        utils.CloneProto(p.decisions[id]),
		})
	}

	return result
}
