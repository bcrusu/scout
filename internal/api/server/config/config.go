package config

import (
	"path"
	"time"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/validation"
	"github.com/google/uuid"
)

var (
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
	ClusterName  string              `yaml:"clusterName" validate:"required,maxLen:100"`
	Server       rpc.ServerConfig    `yaml:"server"`
	InMem        bool                `yaml:"inMem" default:"false"`
	DataDir      string              `yaml:"dataDir" validate:"required"`
	Discovery    discovery.Discovery `yaml:"discovery"`
	Register     Register            `yaml:"register"`
	Session      Session             `yaml:"session"`
	Transactions Transactions        `yaml:"transactions"`
}

type Register struct {
	Token        string        `yaml:"token" default:"GENERATE_RANDOM" validate:"required,maxLen:1024"`
	RetryBackoff utils.Backoff `yaml:"retryBackoff"`
}

type Session struct {
	NewSessionThrottle time.Duration `yaml:"newSessionThrottle" default:"3s" validate:"min:100ms"`
	MaxTimeOffset      time.Duration `yaml:"maxTimeOffset" default:"1s" validate:"min:1ms"`
	HeartbeatInterval  time.Duration `yaml:"heartbeatInterval" default:"5s" validate:"min:100ms"`
	StatusInterval     time.Duration `yaml:"statusInterval" default:"15s" validate:"min:100ms"`
	SendBufferSize     int           `yaml:"sendBufferSize" default:"16" validate:"min:1"`
}

type Transactions struct {
	RetryPolicy utils.RetryPolicy `yaml:"retryPolicy"`
}

func (c *Config) prepare() error {
	if c.Register.Token == "GENERATE_RANDOM" {
		c.Register.Token = uuid.New().String()
	}

	return nil
}

func (c *Config) IdentityFilePath() string {
	return path.Join(c.DataDir, "id")
}
