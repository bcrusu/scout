package partitions

import (
	"context"

	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

type replicaJoining struct {
	id         uint32
	store      storage.Store
	log        logging.Logger
	cancelFunc context.CancelFunc
}

func (p *replicaJoining) Start(ctx context.Context) {
	mainLoop, cancelFunc := utils.WithCancelAndWait(p.mainLoop)

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
}

func (p *replicaJoining) Stop() {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
}

func (p *replicaJoining) mainLoop(ctx context.Context) {
	// TODO
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func (p *replicaJoining) IsDone() bool {
	return false
}
