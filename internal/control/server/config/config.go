package config

import (
	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
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
	Server    rpc.ServerConfig `yaml:"server"`
	Service   Service          `yaml:"service"`
	DataDir   string           `yaml:"dataDir" validate:"required"`
	Register  *Register        `yaml:"register"`
	Bootstrap *Bootstrap       `yaml:"bootstrap"`
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

func (c Config) Validate() error {
	if c.Register != nil && c.Bootstrap != nil {
		return errors.Error("register and bootstrap are mutually exclusive")
	}

	return nil
}
