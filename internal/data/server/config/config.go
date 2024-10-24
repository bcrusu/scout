package config

import (
	"path"
	"path/filepath"
	"time"

	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/multiraft"
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
	Server       rpc.ServerConfig    `yaml:"server"`
	DataDir      string              `yaml:"dataDir" validate:"required"`
	Discovery    discovery.Discovery `yaml:"discovery"`
	Register     Register            `yaml:"register"`
	Session      Session             `yaml:"session"`
	Raft         multiraft.Config    `yaml:"raft"`
	DB           DB                  `yaml:"db"`
	Transactions Transactions        `yaml:"transactions"`
}

type Register struct {
	RetryBackoff utils.Backoff `yaml:"retryBackoff"`
}

type Session struct {
	NewSessionThrottle time.Duration `yaml:"newSessionThrottle" default:"3s" validate:"min:100ms"`
	MaxTimeOffset      time.Duration `yaml:"maxTimeOffset" default:"1s" validate:"min:10ms"`
	HeartbeatInterval  time.Duration `yaml:"heartbeatInterval" default:"5s" validate:"min:100ms"`
	StatusInterval     time.Duration `yaml:"statusInterval" default:"15s" validate:"min:100ms"`
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

func (c *Config) prepare() error {
	dataDir, err := filepath.Abs(c.DataDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine data dir")
	}

	c.DB.RocksDB.DataDir = path.Join(dataDir, "rocksdb")

	return utils.MkdirsAll(
		c.DB.RocksDB.DataDir,
	)
}
