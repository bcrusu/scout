package inmem_test

import (
	"testing"

	"github.com/bcrusu/scout/internal/data/server/storage/inmem"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/utils/tests"
)

func TestSuite(t *testing.T) {
	db := inmem.New()

	kv.TestScenarios(db)

	tests.NewSuite(t, "Inmem KV test suite")
}
