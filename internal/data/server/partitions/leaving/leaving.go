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
	cleanupThrottle                = utils.AddJitter(5 * time.Second)
)

type Leaving struct {
	pid        uint32
	replica    string
	multiraft  *multiraft.Multi
	db         *kv.DBBreaker
	log        logging.Logger
	cancelFunc context.CancelFunc
	status     atomic.Pointer[control.DataServerStatus_LeavingStatus]
}

func New(pid uint32, replica string, multiraft *multiraft.Multi, db kv.DB) *Leaving {
	return &Leaving{
		pid:       pid,
		replica:   replica,
		multiraft: multiraft,
		db:        kv.NewDBBreaker(db),
		log:       logging.New("replica_leaving").With("partition", pid, "replica", replica),
	}
}

func (p *Leaving) Start(ctx context.Context) error {
	p.cancelFunc = utils.RunAsync(ctx, p.mainLoop)
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
		Name:          p.replica,
		LeavingStatus: status,
	}
}

func (p *Leaving) setStatus(completed bool) {
	p.status.Store(&control.DataServerStatus_LeavingStatus{
		Completed: completed,
	})
}

func (p *Leaving) cleanup() error {
	if err := p.multiraft.Drop(p.pid); err != nil {
		return errors.Wrap(err, "failed to remove Raft group.")
	}

	p.db.DropPartition(p.pid)
	return nil
}
