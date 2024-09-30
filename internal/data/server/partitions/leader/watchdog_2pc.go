package leader

import (
	"context"
	"slices"
	"time"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/hlc"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ utils.Lifecycle = (*watchdog2PC)(nil)
)

type TxnConfig struct {
	Phase1Timeout time.Duration `yaml:"phase1Timeout" default:"5s" validate:"min:100ms"`
	Phase2Timeout time.Duration `yaml:"phase2Timeout" default:"2s" validate:"min:100ms"`
}

type watchdog2PC struct {
	partitionID uint32
	config      TxnConfig
	store       storage.Store
	dataClient  data.ServiceClient
	log         logging.Logger
	requestCh   chan storage.TxnRunning
	cancelFunc  context.CancelFunc
}

type dogQueue = *utils.Queue[storage.TxnRunning]

func newWatchdog2PC(partitionID uint32, config TxnConfig, store storage.Store, dataClient data.ServiceClient) *watchdog2PC {
	return &watchdog2PC{
		partitionID: partitionID,
		store:       store,
		dataClient:  dataClient,
		log:         logging.WithComponent("2pc_watchdog").With("partition", partitionID),
		requestCh:   make(chan storage.TxnRunning, 1),
	}
}

func (w *watchdog2PC) Start(ctx context.Context) error {
	all, prepared, decided := w.loadPrepared()

	mainLoop, cancelFunc := utils.WithCancelAndWait(func(ctx context.Context) {
		w.mainLoop(ctx, all, prepared, decided)
	})

	w.cancelFunc = cancelFunc
	go mainLoop(ctx)
	return nil
}

func (w *watchdog2PC) Stop() {
	w.cancelFunc()
}

func (w *watchdog2PC) mainLoop(ctx context.Context, all map[storage.TxnId]bool, prepared, decided dogQueue) {
	ticker := time.NewTicker(min(w.config.Phase1Timeout, w.config.Phase2Timeout) / 2)
	defer ticker.Stop()

	timeoutPrepared := func() {
		oldest := hlc.FromTime(time.Now().Add(-w.config.Phase1Timeout))
		for {
			if peek, ok := prepared.PeekFront(); !ok || peek.Timestamp > oldest {
				break
			} else if txn, _ := prepared.PopFront(); !all[txn.Id] {
				continue
			} else {
				// TODO
			}
		}
	}

	finishDecided := func() {
		oldest := hlc.FromTime(time.Now().Add(-w.config.Phase2Timeout))
		for {
			if peek, ok := decided.PeekFront(); !ok || peek.Timestamp > oldest {
				break
			} else if txn, _ := decided.PopFront(); !all[txn.Id] {
				continue
			} else {
				// TODO
			}
		}
	}

	for {
		select {
		case status := <-w.requestCh:
			if _, ok := all[status.Id]; !ok {
				continue
			}

			switch status.State {
			case data.TxnState_Prepared:
				all[status.Id] = true
				prepared.PushBack(status)
			case data.TxnState_Decided:
				all[status.Id] = true
				decided.PushBack(status)
			case data.TxnState_Committed, data.TxnState_Aborted:
				delete(all, status.Id) // the ticker will later clear from queue
			}
		case <-ticker.C:
			timeoutPrepared()
			finishDecided()
		case <-ctx.Done():
			//  TODO
		}
	}
}

// UpdateTxnStatus takes the latest applied status returned by the FSM and
// the prepared txn only fromm Prepare call.
func (w *watchdog2PC) UpdateTxnStatus(status *data.TxnStatus, txn *data.Txn) {
	if status == nil || status.Id.PrincipalPid != w.partitionID {
		return
	}

	s := storage.TxnRunning{
		Id:        storage.NewTxnId(status.Id),
		Timestamp: status.Timestamp,
		State:     status.State,
	}

	if txn != nil {
		s.ParticipantPids = txn.ParticipantPids
	}

	w.requestCh <- s
}

func (w *watchdog2PC) loadPrepared() (map[storage.TxnId]bool, dogQueue, dogQueue) {
	running := w.store.GetTxnRunning()

	all := map[storage.TxnId]bool{}
	prepared := make([]storage.TxnRunning, 0, len(running))
	decided := make([]storage.TxnRunning, 0, len(running))

	for _, p := range running {
		all[p.Id] = true

		switch p.State {
		case data.TxnState_Prepared:
			prepared = append(prepared, p)
		case data.TxnState_Decided:
			decided = append(decided, p)
		case data.TxnState_Timedout:
			//TODO
		}
	}

	cmpFn := func(a, b storage.TxnRunning) int { return int(a.Timestamp - b.Timestamp) }
	slices.SortFunc(prepared, cmpFn)
	slices.SortFunc(decided, cmpFn)

	return all, utils.NewQueue(prepared...), utils.NewQueue(decided...)
}
