package main

import (
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
