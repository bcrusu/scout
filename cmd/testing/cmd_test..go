package main

import (
	"path"

	"github.com/bcrusu/scout/internal/testing"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newTestCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "test",
		Aliases:       []string{"t"},
		Short:         "Test control.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	c.PersistentFlags().String("work-dir", "", "Current dir if not specified.")

	c.AddCommand(
		newTestRunCmd(),
	)

	return c
}

func getTestConfig(c *cobra.Command) (testing.Config, error) {
	socketPath, err := getSocketPath(c)
	if err != nil {
		return testing.Config{}, err
	}

	workDir, err := getWorkDir(c)
	if err != nil {
		return testing.Config{}, err
	}

	runsDir := path.Join(workDir, "runs")

	if err := utils.MkdirsAll(runsDir); err != nil {
		return testing.Config{}, err
	}

	return testing.Config{
		SocketPath: socketPath,
		RunsDir:    runsDir,
	}, nil
}
