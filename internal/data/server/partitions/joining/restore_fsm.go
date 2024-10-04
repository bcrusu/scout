package joining

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bcrusu/graph/internal/data"
	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_ multiraft.FSM = (*restoreFsm)(nil)
)

// restoreFsm helps the joining replica seed its initial state by streaming
// the partition key-value db contents from a up-to-date sponsor replica. It
// blocks forever inside the Restore method so no raft log entries are apllied.
// Once the operation is complete, it notifies the replica to update its status
// to joining==done. This eventually leads the control plane to transition the
// replica from joining to serving state.
type restoreFsm struct {
	pid        uint32
	localName  string
	dataClient data.ServiceClient
	db         storage.DB
	log        logging.LoggerNoContext
	index      atomic.Uint64
}

func newRestoreFsm(pid uint32, log logging.LoggerNoContext, dataClient data.ServiceClient, db storage.DB) *restoreFsm {
	return &restoreFsm{
		pid:        pid,
		dataClient: dataClient,
		db:         db,
		log:        log,
	}
}

func (f *restoreFsm) Apply(index uint64, _ time.Time, _ []byte) any {
	panic(fmt.Sprintf("unexpected Apply while restoring partition %d replica %s at index %d.", f.pid, f.localName, index))
}

func (f *restoreFsm) Snapshot() ([]byte, error) {
	panic(fmt.Sprintf("unexpected Snapshot while restoring partition %d replica %s.", f.pid, f.localName))
}

func (f *restoreFsm) Restore(snapshot []byte) error {
	snap, err := utils.UnmarshalProto[storage.Snapshot](snapshot)
	if err != nil {
		return err
	}

	f.index.Store(snap.Index)

	// TODO

	return nil
}
