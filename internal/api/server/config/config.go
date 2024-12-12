package config

import (
	"path"
	"path/filepath"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/http"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/metrics"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/session"
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
	ClusterName  string              `yaml:"clusterName" validate:"required,maxLen:100"`
	RPC          rpc.ServerConfig    `yaml:"rpc"`
	HTTP         http.ServerConfig   `yaml:"http"`
	InMem        bool                `yaml:"inMem" default:"false"`
	DataDir      string              `yaml:"dataDir"`
	Discovery    discovery.Discovery `yaml:"discovery"`
	Register     Register            `yaml:"register"`
	Session      session.Config      `yaml:"session"`
	Transactions Transactions        `yaml:"transactions"`
	ProxyMode    bool                `yaml:"proxyMode" default:"true"`
	LogLevels    string              `yaml:"logLevels" default:"*:info"`
	Metrics      metrics.Config      `yaml:"metrics"`
	identityFile string
}

type Register struct {
	Token        string        `yaml:"token" validate:"maxLen:1024"`
	Tags         []string      `yaml:"tags,flow" validate:"maxLen:10,maxItemLen:128"`
	RetryBackoff utils.Backoff `yaml:"retryBackoff"`
}

type Transactions struct {
	RetryPolicy utils.RetryPolicy `yaml:"retryPolicy"`
}

func (c Config) Validate() error {
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

	if c.Register.Token == "" {
		c.Register.Token = uuid.New().String()
	}

	c.RPC.ClusterName = c.ClusterName
	c.RPC.EnableHlc = false
	c.Session.Address = c.RPC.Address

	for i, server := range c.Discovery.Servers {
		c.Discovery.Servers[i] = utils.EnsureAddressPort(server, rpc.DefaultPort)
	}

	hlc.Set(hlc.New(c.Session.MaxTimeOffset))
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

	c.identityFile = path.Join(c.DataDir, "id")

	return utils.MkdirsAll(
		dataDir,
	)
}

func (c Config) IdentityFile() string {
	return c.identityFile
}
