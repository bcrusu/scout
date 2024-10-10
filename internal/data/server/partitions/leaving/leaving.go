package leaving

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data/server/partitions/shared"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_               shared.Replica = (*Leaving)(nil)
	cleanupThrottle                = utils.AddJitter(5*time.Second, 0.15)
)

type Leaving struct {
	pid          uint32
	partName     string
	localReplica string
	multiraft    *multiraft.MultiRaft
	db           *kv.DBBreaker
	log          logging.Logger
	cancelFunc   context.CancelFunc
	status       atomic.Pointer[control.DataServerStatus_LeavingStatus]
}

func New(pid uint32, partName, localReplica string, multiraft *multiraft.MultiRaft, db kv.DB) *Leaving {
	return &Leaving{
		pid:          pid,
		partName:     partName,
		localReplica: localReplica,
		multiraft:    multiraft,
		db:           kv.NewDBBreaker(db),
		log:          logging.WithComponent("replica_leaving").With("partition", pid, "replica", localReplica),
	}
}

func (p *Leaving) Start(ctx context.Context) error {
	mainLoop, cancelFunc := utils.WithCancelAndWait(p.mainLoop)

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (p *Leaving) Stop() {
	p.cancelFunc()
}

func (p *Leaving) mainLoop(ctx context.Context) {
	p.setStatus(false)

	for {
		if err := p.cleanup(); err != nil {
			p.log.WithError(err).Error(ctx, "Cleanup failed. Retrying...")
		} else {
			p.setStatus(true)
			break
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(cleanupThrottle):
		}
	}

	<-ctx.Done()
}

func (p *Leaving) GetService() shared.Service {
	return nil
}

func (p *Leaving) GetStatus() *control.DataServerStatus_Replica {
	status := p.status.Load()
	if status == nil {
		return nil
	}

	return &control.DataServerStatus_Replica{
		Name:          p.localReplica,
		LeavingStatus: status,
	}
}

func (p *Leaving) setStatus(completed bool) {
	p.status.Store(&control.DataServerStatus_LeavingStatus{
		Completed: completed,
	})
}

func (p *Leaving) cleanup() error {
	if err := p.multiraft.Remove(p.partName); err != nil {
		return errors.Wrap(err, "failed to remove Raft group.")
	}

	p.db.DropPartition(p.pid)
	return nil
}
