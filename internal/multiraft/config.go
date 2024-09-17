package multiraft

import (
	"time"

	"github.com/bcrusu/multiraft"
	"github.com/hashicorp/raft"
)

type Config struct {
	BindAddress    string
	RequestTimeout time.Duration
	Transport      multiraft.Transport
	FSM            FSM
}

// TODO: make configurable
func (c Config) getRaftConfig() raft.Config {
	cfg := raft.DefaultConfig()

	cfg.HeartbeatTimeout = 5000 * time.Millisecond
	cfg.ElectionTimeout = 5000 * time.Millisecond
	cfg.LeaderLeaseTimeout = 2000 * time.Millisecond
	cfg.CommitTimeout = 500 * time.Millisecond

	cfg.SnapshotInterval = 5 * time.Minute
	cfg.SnapshotThreshold = 1000
	cfg.TrailingLogs = 10000

	cfg.ShutdownOnRemove = true
	cfg.Logger = newLogAdapter("hashicorp_raft")

	// NoSnapshotRestoreOnStart
	return *cfg
}
