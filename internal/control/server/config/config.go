package config

import (
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
	"github.com/bcrusu/graph/internal/validation"
)

var (
	_ validation.CanValidate = (*Config)(nil)
)

type Config struct {
	Server      rpc.ServerConfig `yaml:"server"`
	Service     Service          `yaml:"service"`
	ClusterName string           `yaml:"clusterName" validate:"required,maxLen:100"`
	DataDir     string           `yaml:"dataDir" validate:"required"`
	Register    *Register        `yaml:"register"`
	Bootstrap   *Bootstrap       `yaml:"bootstrap"`
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
