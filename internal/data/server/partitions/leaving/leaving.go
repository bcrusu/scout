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
	ready      atomic.Bool
}

func New(pid uint32, replica string, multiraft *multiraft.Multi, db kv.DB) *Leaving {
	return &Leaving{
		pid:       pid,
		replica:   replica,
		multiraft: multiraft,
		db:        kv.NewDBBreaker(db),
		log:       logging.New("replica_leaving").With("pid", pid, "replica", replica),
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
	for {
		if err := p.cleanup(); err != nil {
			p.log.WithContext(ctx).WithError(err).Error("Cleanup failed. Retrying...")
		} else {
			p.ready.Store(true)
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
	return &control.DataServerStatus_Replica{
		Name:  p.replica,
		Ready: p.ready.Load(),
	}
}

func (p *Leaving) SetConfig(config *control.DataServerConfig_Partition) {
	// nop
}

func (p *Leaving) cleanup() error {
	if err := p.multiraft.Drop(p.pid); err != nil {
		return errors.Wrap(err, "failed to remove Raft group.")
	}

	p.db.DropPartition(p.pid)
	return nil
}
