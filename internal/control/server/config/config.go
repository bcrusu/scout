package config

import (
	"time"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/validation"
)

var (
	_      validation.CanValidate = (*Config)(nil)
	global *Config
)

func Get() Config {
	if global == nil {
		panic("config was not set")
	}
	return *global
}

func Set(config Config) error {
	if global != nil {
		panic("config already set")
	} else if err := validation.Validate(config); err != nil {
		return err
	}
	global = &config
	return nil
}

type Config struct {
	Server     rpc.ServerConfig `yaml:"server"`
	Service    Service          `yaml:"service"`
	DataDir    string           `yaml:"dataDir" validate:"required"`
	Raft       multiraft.Config `yaml:"raft"`
	Sessions   Sessions         `yaml:"sessions"`
	TimeOffset TimeOffset       `yaml:"timeOffset"`
	Partitions Partitions       `yaml:"partitions"`
	Register   *Register        `yaml:"register"`
	Bootstrap  *Bootstrap       `yaml:"bootstrap"`
}

type Register struct {
	Discovery    discovery.Discovery `yaml:"discovery"`
	RetryBackoff utils.Backoff       `yaml:"retryBackoff"`
}

type Bootstrap struct {
	InitialServers []string      `yaml:"initialServers"`
	PartitionCount uint32        `yaml:"partitionCount" validate:"min:1,max:65536"`
	RetryBackoff   utils.Backoff `yaml:"retryBackoff"`
}

type Service struct {
	ControlClient serviceconfig.Config `yaml:"controlClient"`
	DataClient    serviceconfig.Config `yaml:"dataClient"`
	ApiClient     serviceconfig.Config `yaml:"apiClient"`
}

type Sessions struct {
	ReceiveBurst        int           `yaml:"receiveBurst" default:"5" validate:"min:1"`
	ReceiveMaxOffenses  int           `yaml:"receiveMaxOffenses" default:"16" validate:"min:1"` // After this the session will be closed
	WriteStatusInterval time.Duration `yaml:"writeStatusInterval" default:"5s" validate:"min:100ms"`
	SendBufferSize      int           `yaml:"sendBufferSize" default:"16" validate:"min:1"`
}

type TimeOffset struct {
	MaxTimeOffset        time.Duration `yaml:"maxTimeOffset" default:"1s" validate:"min:10ms"`
	CheckInterval        time.Duration `yaml:"checkInterval" default:"5s" validate:"min:100ms"`
	GlobalTruncationPct  float64       `yaml:"globalTruncationPct" default:"80" validate:"min:1"`
	GlobalWarmupCount    int           `yaml:"globalWarmupCount" default:"100" validate:"min:1"`
	SessionTruncationPct float64       `yaml:"sessionTruncationPct" default:"95" validate:"min:1"`
	SessionWarmupCount   int           `yaml:"sessionWarmupCount" default:"10" validate:"min:1"`
}

type Partitions struct {
	ReplicationFactor      int           `yaml:"replicationFactor" default:"3" validate:"min:1"`
	InitalDelay            time.Duration `yaml:"initalDelay" default:"1m" validate:"min:1s"`
	RebalanceInterval      time.Duration `yaml:"rebalanceInterval" default:"1m" validate:"min:1s"`
	MaxJoining             int           `yaml:"maxJoining" default:"16" validate:"min:1"`
	MaxJoiningForServer    int           `yaml:"maxJoiningForServer" default:"2" validate:"min:1"`
	MaxJoiningForPartition int           `yaml:"maxJoiningForPartition" default:"1" validate:"min:1"`
}

func (c Config) Validate() error {
	if c.Register != nil && c.Bootstrap != nil {
		return errors.Error("register and bootstrap are mutually exclusive")
	}

	return nil
}
