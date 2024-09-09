package multiraft

import (
	"time"

	"github.com/hashicorp/raft"
)

type Config struct {
	ID             string
	Address        string
	RequestTimeout time.Duration
	Transport      raft.Transport
	FSM            FSM
}

// TODO: make configurable
func (c Config) getRaftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(c.ID)

	cfg.HeartbeatTimeout = 5000 * time.Millisecond
	cfg.ElectionTimeout = 5000 * time.Millisecond
	cfg.LeaderLeaseTimeout = 2000 * time.Millisecond
	cfg.CommitTimeout = 500 * time.Millisecond

	cfg.SnapshotInterval = 5 * time.Minute
	cfg.SnapshotThreshold = 1000
	cfg.TrailingLogs = 10000

	cfg.ShutdownOnRemove = true
	cfg.Logger = hcLog

	// NoSnapshotRestoreOnStart
	return cfg
}
