package config

import (
	"time"

	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/validation"
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
	}
	global = &config
	return nil
}

type Config struct {
	Server       rpc.ServerConfig    `yaml:"server"`
	DataDir      string              `yaml:"dataDir" validate:"required"`
	Discovery    discovery.Discovery `yaml:"discovery"`
	Transactions TxnConfig           `yaml:"transactions"`
}

type TxnConfig struct {
	Phase1Timeout time.Duration `yaml:"phase1Timeout" default:"5s" validate:"min:100ms"`
	Phase2Timeout time.Duration `yaml:"phase2Timeout" default:"2s" validate:"min:100ms"`
}
