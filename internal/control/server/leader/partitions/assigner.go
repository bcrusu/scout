package partitions

import (
	"context"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/control/server/storage"
	"github.com/bcrusu/scout/internal/eventbus"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_   utils.Lifecycle = (*Assigner)(nil)
	log                 = logging.New("assigner").NoContext()
)

// Assigner is the one that assigns partitions to servers.
type Assigner struct {
	config     config.Partitions
	store      storage.Store
	cancelFunc context.CancelFunc
}

func NewAssigner(store storage.Store) *Assigner {
	return &Assigner{
		config: config.Get().Partitions,
		store:  store,
	}
}

func (a *Assigner) Start(ctx context.Context) error {
	a.cancelFunc = utils.RunAsync(ctx, a.mainLoop)
	return nil
}

func (a *Assigner) Stop() {
	a.cancelFunc()
}

func (a *Assigner) mainLoop(ctx context.Context) {
	serversSub := eventbus.Subscribe[*control.Servers]()
	defer serversSub.Unsubscribe()

	for {
		select {
		case servers := <-serversSub.Items():
			if x := len(servers.DataServers()); x > 0 {
				log.Infof("Found %d data servers. Starting the assign loop...", x)
				go a.assignLoop(ctx)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (a *Assigner) assignLoop(ctx context.Context) {
	var initTimer *time.Timer
	var rebalanceTicker *time.Ticker

	if !a.store.Partitions().HasAssignments() {
		initTimer = time.NewTimer(a.config.InitDelay)
	} else {
		rebalanceTicker = time.NewTicker(a.config.RebalanceInterval)
	}

	for {
		select {
		case <-utils.GetTimerChan(initTimer):
			a.initAssignments()

			initTimer.Stop()
			initTimer = nil
			rebalanceTicker = time.NewTicker(a.config.RebalanceInterval)
		case <-utils.GetTickerChan(rebalanceTicker):
			if !a.store.Partitions().HasAssignments() {
				a.initAssignments()
			} else {
				a.updateAssignments()
			}
		case <-ctx.Done():
			if initTimer != nil {
				initTimer.Stop()
			}
			if rebalanceTicker != nil {
				rebalanceTicker.Stop()
			}
			return
		}
	}
}
