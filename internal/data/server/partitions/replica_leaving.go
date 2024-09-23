package partitions

import (
	"context"

	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

type replicaLeaving struct {
	id         uint32
	store      storage.Store
	log        logging.Logger
	cancelFunc context.CancelFunc
}

func (p *replicaLeaving) Start(ctx context.Context) {
	mainLoop, cancelFunc := utils.WithCancelAndWait(p.mainLoop)

	p.cancelFunc = cancelFunc
	go mainLoop(ctx)
}

func (p *replicaLeaving) Stop() {
	p.cancelFunc()
}

func (p *replicaLeaving) mainLoop(ctx context.Context) {
	// TODO
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func (p *replicaLeaving) IsDone() bool {
	return false
}
