package rocksdb_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/data/server/storage/rocksdb"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func TestRocksDB(t *testing.T) {
	var (
		dataDir = fmt.Sprintf("./rocksdb_tests_%d", time.Now().Unix())
		db      *rocksdb.RocksDB
	)

	cleanup := func() {
		if db != nil {
			db.Stop()
			os.RemoveAll(dataDir)
			db = nil
		}
	}
	defer cleanup()

	cfg := config.RocksDB{
		DataDir: dataDir,
	}
	utils.SetDefaults(&cfg)
	utils.MkdirAll(dataDir)

	db = rocksdb.NewWithConfig(cfg)
	if err := db.Start(context.Background()); err != nil {
		panic("RocksDB failed to start")
	}

	ginkgo.AfterSuite(func() {
		cleanup()
	})

	gomega.RegisterFailHandler(ginkgo.Fail)

	kv.TestScenarios(db.KV())
	mvcc.TestScenarios(db.KV(), db.MVCC())

	ginkgo.RunSpecs(t, "RocksDB integration test suite")
}
