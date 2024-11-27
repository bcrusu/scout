package main

import (
	"context"
	"os"

	"github.com/bcrusu/scout/internal/logging"
	"github.com/spf13/cobra"
)

var (
	log = logging.New("main")
)

func main() {
	cobra.EnableTraverseRunHooks = true
	cmd := newRootCmd()
	ctx := context.Background()

	if err := cmd.ExecuteContext(ctx); err != nil {
		log.WithError(err).Error("Unexpected error")
		os.Exit(1)
	}
}
