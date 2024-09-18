package multiraft

import (
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/multiraft"
	"github.com/hashicorp/raft"
)

var (
	log = logging.WithComponent("multiraft").NoContext()
)

// MultiRaft allows working with multiple Raft groups.
type MultiRaft struct {
	baseConfig Config
	multi      *multiraft.Raft
}

// NewMultiRaft returns a new MultiRaft instance which allows working with multiple Raft groups.
func NewMultiRaft(baseConfig Config) *MultiRaft {
	logs := multiraft.NewLogStore(newLogAdapter("raft_log_store"))
	stable := multiraft.NewStableStore(newLogAdapter("raft_stable_store"))
	snapshot := multiraft.NewSnapshotStore(newLogAdapter("raft_snapshot_store"))

	multi := multiraft.NewRaft(logs, stable, snapshot, baseConfig.Transport)

	return &MultiRaft{
		baseConfig: baseConfig,
		multi:      multi,
	}
}

func (r *MultiRaft) New(groupID string, fsm FSM, localID raft.ServerID) (*Raft, error) {
	leaderChan := make(chan bool, 1)

	config := r.baseConfig.getRaftConfig()
	config.LocalID = localID
	config.NotifyCh = leaderChan

	fsmAdapter := &fsmAdapter{fsm}
	group, err := r.multi.New(groupID, fsmAdapter, config)
	if err != nil {
		return nil, err
	}

	return &Raft{
		localID:        localID,
		bindAddress:    raft.ServerAddress(r.baseConfig.BindAddress),
		requestTimeout: r.baseConfig.RequestTimeout,
		raft:           group,
		leaderChan:     leaderChan,
	}, nil
}

func (r *MultiRaft) HasExistingState(groupID string) (bool, error) {
	return r.multi.HasExistingState(groupID)
}
