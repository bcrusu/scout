package config

import (
	"path"
	"path/filepath"
	"time"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/validation"
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
	Server        rpc.ServerConfig    `yaml:"server"`
	DataDir       string              `yaml:"dataDir" validate:"required"`
	Discovery     discovery.Discovery `yaml:"discovery"`
	Transactions  TxnConfig           `yaml:"transactions"`
	DBRetryPolicy utils.RetryPolicy   `yaml:"dbRetryPolicy"`
	RocksDB       RocksDBConfig       `yaml:"rocksDB"`
}

type TxnConfig struct {
	Phase1Timeout     time.Duration     `yaml:"phase1Timeout" default:"5s" validate:"min:100ms"`
	Phase2Timeout     time.Duration     `yaml:"phase2Timeout" default:"2s" validate:"min:100ms"`
	RetryPolicy       utils.RetryPolicy `yaml:"retryPolicy"`
	RetryBreakerLimit int               `yaml:"retryBreakerLimit" default:"32" validate:"min:1"`
	BatchMaxSize      int               `yaml:"batchMaxSize" default:"128" validate:"min:1"`
	BatchMaxDelay     time.Duration     `yaml:"batchMaxDelay" default:"100ms" validate:"min:1ms"`
}

type RocksDBConfig struct {
	DataDir   string
	CacheSize utils.Bytes   `yaml:"cacheSize" default:"1GB" validate:"min:1MB"`
	TTL       time.Duration `yaml:"ttl" default:"24h" validate:"min:1m"` // TODO
}

func (c *Config) prepare() error {
	dataDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine data dir ")
	}

	c.RocksDB.DataDir = path.Join(dataDir, "rocksdb")

	return utils.MkdirsAll(
		c.RocksDB.DataDir,
	)
}
