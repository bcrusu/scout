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
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/rpc"
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
	Session      Session             `yaml:"session"`
	Raft         multiraft.Config    `yaml:"raft"`
	DB           DB                  `yaml:"db"`
	Transactions Transactions        `yaml:"transactions"`
	LogLevels    string              `yaml:"logLevels" default:"*:info"`
	identityFile string
	raftDir      string
}

type Register struct {
	Token        string        `yaml:"token" default:"GENERATE_RANDOM" validate:"required,maxLen:1024"`
	Tags         []string      `yaml:"tags" validate:"maxLen:10,maxItemLen:128"`
	RetryBackoff utils.Backoff `yaml:"retryBackoff"`
}

type Session struct {
	NewSessionThrottle time.Duration `yaml:"newSessionThrottle" default:"3s" validate:"min:100ms"`
	MaxTimeOffset      time.Duration `yaml:"maxTimeOffset" default:"1s" validate:"min:10ms"`
	HeartbeatInterval  time.Duration `yaml:"heartbeatInterval" default:"5s" validate:"min:100ms"`
	StatusInterval     time.Duration `yaml:"statusInterval" default:"3s" validate:"min:100ms"`
	SendBufferSize     int           `yaml:"sendBufferSize" default:"16" validate:"min:1"`
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
	CleanAfterReadWrite time.Duration     `yaml:"cleanAfterReadWrite" default:"30m" validate:"min:1m"`
	CleanAfterReadOnly  time.Duration     `yaml:"cleanAfterReadOnly" default:"30s" validate:"min:100ms"`
}

type DB struct {
	RetryPolicy       utils.RetryPolicy `yaml:"retryPolicy"`
	MaxStreamingSize  int               `yaml:"maxStreamingSize" default:"10000" validate:"min:100"`
	StreamingThrottle time.Duration     `yaml:"streamingThrottle" default:"5s" validate:"min:100ms"`
	RocksDB           RocksDB           `yaml:"rocksDB"`
}

type RocksDB struct {
	DataDir               string
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

	if c.Register.Token == "GENERATE_RANDOM" {
		c.Register.Token = uuid.New().String()
	}

	c.RPC.ClusterName = c.ClusterName
	c.RPC.EnableHlc = true

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
