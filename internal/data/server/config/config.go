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
	Raft         multiraft.Config    `yaml:"raft"`
	DB           DB                  `yaml:"db"`
	Transactions Transactions        `yaml:"transactions"`
	LogLevels    string              `yaml:"logLevels" default:"*:info"`
	Metrics      metrics.Config      `yaml:"metrics"`
	identityFile string
	raftDir      string
}

type Register struct {
	Token        string        `yaml:"token,omitempty" validate:"maxLen:1024"`
	Tags         []string      `yaml:"tags,flow" validate:"maxLen:10,maxItemLen:128"`
	RetryBackoff utils.Backoff `yaml:"retryBackoff"`
}

type Transactions struct {
	Phase1Timeout       time.Duration     `yaml:"phase1Timeout" default:"5s" validate:"min:100ms"`
	Phase2Timeout       time.Duration     `yaml:"phase2Timeout" default:"2s" validate:"min:100ms"`
	RetryPolicy         utils.RetryPolicy `yaml:"retryPolicy"`
	RetryBreakerLimit   int               `yaml:"retryBreakerLimit" default:"32" validate:"min:1"`
	MaxBatchSize        int               `yaml:"maxBatchSize" default:"128" validate:"min:1"`
	MaxBatchDelay       time.Duration     `yaml:"maxBatchDelay" default:"100ms" validate:"min:1ms"`
	MaxIteratorResults  int               `yaml:"maxIteratorResults" default:"1000" validate:"min:100"`
	SkipCorruptedData   bool              `yaml:"skipCorruptedData" default:"true"`
	CleanAfterReadWrite time.Duration     `yaml:"cleanAfterReadWrite" default:"1m" validate:"min:1s"`
	CleanAfterReadOnly  time.Duration     `yaml:"cleanAfterReadOnly" default:"10s" validate:"min:100ms"`
}

type DB struct {
	RetryPolicy       utils.RetryPolicy `yaml:"retryPolicy"`
	MaxStreamingSize  int               `yaml:"maxStreamingSize" default:"10000" validate:"min:100"`
	StreamingThrottle time.Duration     `yaml:"streamingThrottle" default:"5s" validate:"min:100ms"`
	RocksDB           RocksDB           `yaml:"rocksDB"`
}

type RocksDB struct {
	DataDir               string        `yaml:"-"`
	WriteBufferSize       utils.Bytes   `yaml:"writeBufferSize" default:"128MB" validate:"min:32MB"`
	CacheSize             utils.Bytes   `yaml:"cacheSize" default:"1GB" validate:"min:1MB"`
	TTL                   time.Duration `yaml:"ttl" default:"24h" validate:"min:1m"` // TODO
	MaxReadaheadSize      utils.Bytes   `yaml:"maxReadaheadSize" default:"32MB" validate:"min:1KB"`
	MaxKeyPrefixLen       int           `yaml:"maxKeyPrefixLen" default:"10" validate:"min:5"` // key prefix for table bloom filter
	BloomFilterBitsPerKey float64       `yaml:"bloomFilterBitsPerKey" default:"10" validate:"min:1"`
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
	c.RPC.EnableHlc = true
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

	c.identityFile = path.Join(dataDir, "id")
	c.raftDir = path.Join(dataDir, "raft")
	c.DB.RocksDB.DataDir = path.Join(dataDir, "rocksdb")

	return utils.MkdirsAll(
		c.raftDir,
		c.DB.RocksDB.DataDir,
	)
}

func (c Config) IdentityFile() string {
	return c.identityFile
}

func (c *Config) RaftDir() string {
	return c.raftDir
}
