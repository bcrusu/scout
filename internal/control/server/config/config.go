package config

import (
	"time"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
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
	Server        rpc.ServerConfig `yaml:"server"`
	Service       Service          `yaml:"service"`
	DataDir       string           `yaml:"dataDir" validate:"required"`
	MaxTimeOffset time.Duration    `yaml:"maxTimeOffset" default:"1s" validate:"min:1ms"`
	Raft          multiraft.Config `yaml:"raft"`
	Sessions      Sessions         `yaml:"sessions"`
	Partitions    Partitions       `yaml:"partitions"`
	Register      *Register        `yaml:"register"`
	Bootstrap     *Bootstrap       `yaml:"bootstrap"`
}

type Register struct {
	Discovery discovery.Discovery `yaml:"discovery"`
}

type Bootstrap struct {
	InitialServers []string `yaml:"initialServers"`
	PartitionCount uint32   `yaml:"partitionCount" validate:"min:1,max:65536"`
}

type Service struct {
	ControlClient serviceconfig.Config `yaml:"controlClient"`
	DataClient    serviceconfig.Config `yaml:"dataClient"`
	ApiClient     serviceconfig.Config `yaml:"apiClient"`
}

type Sessions struct {
	ReceiveBurst            int           `yaml:"receiveBurst" default:"5" validate:"min:1"`
	ReceiveMaxOffenses      int           `yaml:"receiveMaxOffenses" default:"16" validate:"min:1"` // After this the session will be closed
	TimeOffsetCheckInterval time.Duration `yaml:"timeOffsetCheckInterval" default:"5s" validate:"min:100ms"`
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
