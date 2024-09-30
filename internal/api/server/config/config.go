package config

import (
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/utils"
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
	RetryPolicy utils.RetryPolicy `yaml:"retryPolicy" default:"3" validate:"min:1,max:10"`
}
