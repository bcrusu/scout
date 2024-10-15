package serving

import (
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ txn.RaftStore = (*raftStore)(nil)
)

type raftStore struct {
	raft *multiraft.Raft
}

func (s *raftStore) ApplyBatch(batch *txn.Batch) (<-chan multiraft.AsyncResult, error) {
	cmd := &storage.Command{Payload: &storage.Command_Batch{Batch: batch}}

	data, err := utils.MarshalProto(cmd)
	if err != nil {
		return nil, err
	}

	return s.raft.ApplyAsync(data), nil
}
