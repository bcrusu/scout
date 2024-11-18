package main

import (
	"context"
	"os"
	"runtime"

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

	if num := runtime.NumGoroutine(); num > 1 {
		log.Warnf("NumGoroutine count %d", num)
	}

	log.Info("Done")
}
