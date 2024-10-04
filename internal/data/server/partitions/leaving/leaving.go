package leaving

import (
	"context"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data/server/partitions/shared"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ shared.Replica = (*Leaving)(nil)
)

type Leaving struct {
	pid          uint32
	localReplica string
	log          logging.Logger
	getStatusCh  chan chan<- *control.DataServerStatus_Replica
	cancelFunc   context.CancelFunc
}

func New(pid uint32, localReplica string) *Leaving {
	return &Leaving{
		pid:          pid,
		localReplica: localReplica,
		log:          logging.WithComponent("replica_leaving").With("partition", pid, "replica", localReplica),
		getStatusCh:  make(chan chan<- *control.DataServerStatus_Replica),
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
	for {
		select {
		case statusCh := <-p.getStatusCh:
			statusCh <- &control.DataServerStatus_Replica{
				Name:        p.localReplica,
				DoneLeaving: false, // TODO
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *Leaving) GetService() shared.Service {
	return nil
}

func (p *Leaving) GetStatus() *control.DataServerStatus_Replica {
	statusCh := make(chan *control.DataServerStatus_Replica, 1)
	p.getStatusCh <- statusCh
	return <-statusCh
}
