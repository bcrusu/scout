package multiraft

import (
	"context"

	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	_   utils.Lifecycle = (*MultiRaft)(nil)
	log                 = logging.WithComponent("multiraft").NoContext()
)

// MultiRaft allows working with multiple Raft groups.
type MultiRaft struct {
	groups map[uint]*Raft
}

// TODO: rename
type Leader struct {
	Raft     *Raft
	GroupID  uint
	IsLeader bool
}

// NewMultiRaft returns a new MultiRaft instance which allows working with multiple
// Raft groups while sharing disk and network resources:
//   - single LogStore to store write-ahead logs
//   - single Transport which reuses the same connection between server pairs, multiplexing RPC calls
//   - single SnapshotStore for storing snapshots
//   - single StableStore for storing group configuration
func NewMultiRaft() *MultiRaft {
	return nil
}

// Start starts the show.
func (r *MultiRaft) Start(ctx context.Context) error {
	return nil
}

// Stop stops the show.
func (r *MultiRaft) Stop(ctx context.Context) {
}

func (r *MultiRaft) GetLeaderChan() <-chan Leader {
	return nil
}
