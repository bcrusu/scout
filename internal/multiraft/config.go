package multiraft

import (
	"time"

	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/hashicorp/raft"
)

type Config struct {
	HeartbeatTimeout   time.Duration        `yaml:"heartbeatTimeout" default:"3s" validate:"min:100ms"`
	ElectionTimeout    time.Duration        `yaml:"electionTimeout" default:"5s" validate:"min:100ms"`
	LeaderLeaseTimeout time.Duration        `yaml:"leaderLeaseTimeout" default:"3s" validate:"min:100ms"`
	CommitTimeout      time.Duration        `yaml:"commitTimeout" default:"500ms" validate:"min:100ms"`
	RequestTimeout     time.Duration        `yaml:"requestTimeout" default:"1s" validate:"min:100ms"`
	SnapshotInterval   time.Duration        `yaml:"snapshotInterval" default:"5m" validate:"min:10s"`
	SnapshotThreshold  uint64               `yaml:"snapshotThreshold" default:"250" validate:"min:20"`
	TrailingLogs       uint64               `yaml:"trailingLogs" default:"100" validate:"min:20"`
	TransportClient    serviceconfig.Config `yaml:"transportClient"`
}

func (c Config) getRaftConfig() raft.Config {
	cfg := raft.DefaultConfig()

	cfg.HeartbeatTimeout = c.HeartbeatTimeout
	cfg.ElectionTimeout = c.ElectionTimeout
	cfg.LeaderLeaseTimeout = c.LeaderLeaseTimeout
	cfg.CommitTimeout = c.CommitTimeout

	cfg.SnapshotInterval = c.SnapshotInterval
	cfg.SnapshotThreshold = c.SnapshotThreshold
	cfg.TrailingLogs = c.TrailingLogs

	cfg.ShutdownOnRemove = true
	cfg.PreVoteDisabled = false
	cfg.Logger = newLogAdapter("hashicorp_raft")

	return *cfg
}
