package main

import (
	"github.com/bcrusu/scout/internal/testing"
	"github.com/spf13/cobra"
)

func newTestRunCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "run",
		Aliases:       []string{"r"},
		Short:         "Executes a single test run.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			config, err := getTestConfig(c)
			if err != nil {
				return err
			}

			return testing.Run(c.Context(), config)
		},
	}

	return c
}
