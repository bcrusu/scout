package common

import (
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
)

var (
	log = logging.WithComponent("data_common")
)

// Shared implements common functionality for both leader and follower roles.
type Shared struct {
	raft *multiraft.Raft
}

func New(raft *multiraft.Raft) *Shared {
	return &Shared{
		raft: raft,
	}
}
