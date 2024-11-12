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

// NewLeader, for now, performs an empty barrier write which has multiple purposes:
//   - it ensures that the FSM has processed all outstanding commited logs, thus it
//     has observed all txn timestamps and updated the HLC accordingly.
//   - it kickstarts the partition log with the first write to help joining replicas
//     make progress when the log is empty (check the restore FSM notes for details).
//   - and possible other scenarios that require action/s on leader change. Might even
//     evolve into a leadership "lease" system.
func (s *raftStore) NewLeader() error {
	cmd := &storage.Command{Payload: &storage.Command_Barrier{Barrier: &storage.Barrier{}}}
	data := errors.Assert2(utils.MarshalProto(cmd))

	if _, err := s.raft.Apply(data); err != nil {
		return errors.Wrap(err, "new leader barrier failed")
	}

	return nil
}

func (s *raftStore) ApplyBatch(batch *data.TxnBatch) <-chan multiraft.AsyncResult {
	cmd := &storage.Command{Payload: &storage.Command_TxnBatch{TxnBatch: batch}}
	data := errors.Assert2(utils.MarshalProto(cmd))

	return s.raft.ApplyAsync(data)
}
