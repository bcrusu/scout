package mvcc_test

import (
	"testing"

	"github.com/bcrusu/scout/internal/data/server/storage/inmem"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/utils/tests"
)

func TestSuite(t *testing.T) {
	kvdb := inmem.New()
	db := mvcc.NewEmulated(kvdb)

	mvcc.TestScenarios(kvdb, db)

	tests.NewSuite(t, "Emulated MVCC test suite")
}
