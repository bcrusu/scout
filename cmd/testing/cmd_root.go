package main

import (
	"github.com/bcrusu/scout/internal/logging"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "testing",
		Short:         "Scout tests helper command.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			logLevels, err := c.Flags().GetString("log-levels")
			if err != nil {
				return err
			} else if err := logging.SetLevels(logLevels); err != nil {
				return err
			}

			return nil
		},
	}

	c.PersistentFlags().String("log-levels", "*:info", "Log levels.")

	c.AddCommand(
		newAgentCmd(),
		newNodeCmd(),
		newServiceCmd(),
		newTestCmd(),
	)

	return c
}
