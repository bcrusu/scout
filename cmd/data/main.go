package main

import (
	"context"
	"os"

	"github.com/bcrusu/graph/internal/logging"
	"github.com/spf13/cobra"
)

func main() {
	cobra.EnableTraverseRunHooks = true
	cmd := newRootCmd()
	ctx := context.Background()
	log := logging.WithComponent("main")

	if err := cmd.ExecuteContext(ctx); err != nil {
		log.WithError(err).Error(ctx, "Unexpected error")
		os.Exit(1)
	}

	log.Info(ctx, "Done")
}
