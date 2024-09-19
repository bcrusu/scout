package config

import (
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/bcrusu/graph/internal/rpc/serviceconfig"
)

type Config struct {
	Server      rpc.ServerConfig    `yaml:"server"`
	Service     Service             `yaml:"service"`
	ClusterName string              `yaml:"clusterName"`
	DataDir     string              `yaml:"dataDir"`
	Discovery   discovery.Discovery `yaml:"discovery"`
}

type Service struct {
	ControlClient serviceconfig.Config `yaml:"controlClient"`
	DataClient    serviceconfig.Config `yaml:"dataClient"`
	ApiClient     serviceconfig.Config `yaml:"apiClient"`
}
