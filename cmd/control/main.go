package main

import (
	"context"
	"os"

	"github.com/bcrusu/scout/internal/logging"
	"github.com/spf13/cobra"
)

func main() {
	cobra.EnableTraverseRunHooks = true
	cmd := newCmd()
	ctx := context.Background()
	log := logging.New("main")

	if err := cmd.ExecuteContext(ctx); err != nil {
		log.WithError(err).Error("Unexpected error")
		os.Exit(1)
	}
}
