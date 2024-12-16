package config

import (
	"path"
	"path/filepath"
	"time"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/http"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/metrics"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/rpc/serviceconfig"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/validation"
	"github.com/google/uuid"
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
	} else if err := config.prepare(); err != nil {
		return err
	}

	global = &config
	return nil
}

type Config struct {
	ClusterName string            `yaml:"clusterName" validate:"required,maxLen:100"`
	RPC         rpc.ServerConfig  `yaml:"rpc"`
	HTTP        http.ServerConfig `yaml:"http"`
	Service     Service           `yaml:"service"`
	InMem       bool              `yaml:"inMem" default:"false"`
	DataDir     string            `yaml:"dataDir"`
	Raft        multiraft.Config  `yaml:"raft"`
	Sessions    Sessions          `yaml:"sessions"`
	TimeOffset  TimeOffset        `yaml:"timeOffset"`
	Partitions  Partitions        `yaml:"partitions"`
	Register    *Register         `yaml:"register"`
	Bootstrap   *Bootstrap        `yaml:"bootstrap"`
	LogLevels   string            `yaml:"logLevels" default:"*:info"`
	Metrics     metrics.Config    `yaml:"metrics"`
}

type Register struct {
	Token        string              `yaml:"token,omitempty" validate:"required,maxLen:1024"`
	Tags         []string            `yaml:"tags,flow" validate:"maxLen:10,maxItemLen:128"`
	Discovery    discovery.Discovery `yaml:"discovery"`
	RetryBackoff utils.Backoff       `yaml:"retryBackoff"`
}

type InitialServer struct {
	Address string   `yaml:"address" validate:"maxLen:128"`
	Tags    []string `yaml:"tags,flow" validate:"maxLen:10,maxItemLen:128"`
}

type Bootstrap struct {
	InitialServers []InitialServer `yaml:"initialServers"`
	PartitionCount uint32          `yaml:"partitionCount" validate:"min:1,max:65536"`
	RetryBackoff   utils.Backoff   `yaml:"retryBackoff"`
}

type Service struct {
	Control serviceconfig.Config `yaml:"control"`
	Data    serviceconfig.Config `yaml:"data"`
	Api     serviceconfig.Config `yaml:"api"`
}

type Sessions struct {
	ReceiveBurst        int           `yaml:"receiveBurst" default:"5" validate:"min:1"`
	ReceiveMaxOffenses  int           `yaml:"receiveMaxOffenses" default:"16" validate:"min:1"` // After this the session will be closed
	WriteStatusInterval time.Duration `yaml:"writeStatusInterval" default:"5s" validate:"min:100ms"`
	SendBufferSize      int           `yaml:"sendBufferSize" default:"16" validate:"min:5"`
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
	InitDelay              time.Duration `yaml:"initDelay" default:"10s" validate:"min:1s"`
	RebalanceInterval      time.Duration `yaml:"rebalanceInterval" default:"30s" validate:"min:1s"`
	MaxJoining             int           `yaml:"maxJoining" default:"16" validate:"min:1"`
	MaxJoiningForServer    int           `yaml:"maxJoiningForServer" default:"2" validate:"min:1"`
	MaxJoiningForPartition int           `yaml:"maxJoiningForPartition" default:"1" validate:"min:1"`
}

func (c Config) Validate() error {
	if c.Register != nil && c.Bootstrap != nil {
		return errors.Error("register and bootstrap are mutually exclusive")
	}

	if !c.InMem && c.DataDir == "" {
		return errors.Error("missing data dir")
	}

	return nil
}

func (c *Config) prepare() error {
	if err := logging.SetLevels(c.LogLevels); err != nil {
		return errors.Wrap(err, "failed to set log levels")
	}

	bindAddress, err := utils.GetBindAddress()
	if err != nil {
		return err
	}

	if c.RPC.Address == "" {
		c.RPC.Address = utils.JoinHostPort(bindAddress, rpc.DefaultPort)
	} else {
		c.RPC.Address = utils.EnsureAddressPort(c.RPC.Address, rpc.DefaultPort)
	}

	if c.HTTP.Address == "" {
		c.HTTP.Address = utils.JoinHostPort(bindAddress, http.DefaultPort)
	} else {
		c.HTTP.Address = utils.EnsureAddressPort(c.HTTP.Address, http.DefaultPort)
	}

	if c.Bootstrap != nil {
		for i, server := range c.Bootstrap.InitialServers {
			addr, err := utils.LookupHost(server.Address)
			if err != nil {
				return err
			}

			c.Bootstrap.InitialServers[i].Address = utils.EnsureAddressPort(addr, rpc.DefaultPort)
		}
	}

	if c.Register != nil {
		if c.Register.Token == "" {
			c.Register.Token = uuid.New().String()
		}

		for i, server := range c.Register.Discovery.Servers {
			c.Register.Discovery.Servers[i] = utils.EnsureAddressPort(server, rpc.DefaultPort)
		}
	}

	c.RPC.ClusterName = c.ClusterName
	c.RPC.EnableHlc = true

	hlc.Set(hlc.New(c.TimeOffset.MaxTimeOffset))
	return c.prepareDirs()
}

func (c *Config) prepareDirs() error {
	if c.InMem {
		return nil
	}

	dataDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine data dir")
	}

	return utils.MkdirsAll(
		path.Join(dataDir, "raft"),
	)
}

func (c Config) IdentityFile() string {
	return path.Join(c.DataDir, "id")
}

func (c Config) RaftDir() string {
	return path.Join(c.DataDir, "raft")
}
