package serving

import (
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ txn.RaftStore = (*raftStore)(nil)
)

type raftStore struct {
	raft *multiraft.Raft
	log  logging.Logger
}

func newRaftStore(pid uint32, replica string, raft *multiraft.Raft) *raftStore {
	return &raftStore{
		raft: raft,
		log:  logging.New("raft_store").With("partition", pid, "replica", replica),
	}
}

// NewLeader, for now, performs an empty barrier write to kickstart the partition log.
// This is especially helpful for joining replicas that cannot make progress if the
// log is empty (check the restore FSM notes). In the future, this approach could
// evolve into a leader "lease" system.
func (s *raftStore) NewLeader() {
	cmd := &storage.Command{Payload: &storage.Command_Barrier{Barrier: &storage.Barrier{}}}
	data := errors.Assert2(utils.MarshalProto(cmd))

	if _, err := s.raft.Apply(data); err != nil {
		s.log.WithError(err).Error("New leader barrier failed.")
	}
}

func (s *raftStore) ApplyBatch(batch *data.TxnBatch) <-chan multiraft.AsyncResult {
	cmd := &storage.Command{Payload: &storage.Command_TxnBatch{TxnBatch: batch}}
	data := errors.Assert2(utils.MarshalProto(cmd))

	return s.raft.ApplyAsync(data)
}
