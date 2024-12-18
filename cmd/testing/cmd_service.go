package main

import (
	"context"

	"github.com/bcrusu/scout/internal/testing/services"
	"github.com/spf13/cobra"
)

func newServiceCmd() *cobra.Command {
	type action func(context.Context, string) error

	run := func(c *cobra.Command, action action) error {
		socketPath, err := getSocketPath(c)
		if err != nil {
			return err
		}
		return action(c.Context(), socketPath)
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start services.",
		RunE: func(c *cobra.Command, args []string) error {
			return run(c, services.Start)
		},
	}

	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop services.",
		RunE: func(c *cobra.Command, args []string) error {
			return run(c, services.Stop)
		},
	}

	restartCmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart services.",
		RunE: func(c *cobra.Command, args []string) error {
			return run(c, services.Restart)
		},
	}

	resetCmd := &cobra.Command{
		Use:   "reset",
		Short: "Stop services and remove the persisted state.",
		RunE: func(c *cobra.Command, args []string) error {
			return run(c, services.Reset)
		},
	}

	c := &cobra.Command{
		Use:           "service",
		Aliases:       []string{"s", "svc"},
		Short:         "Service operations.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	c.PersistentFlags().String("work-dir", "", "Current dir if not specified.")

	c.AddCommand(
		newServiceConfigCmd(),
		newServiceLogsCmd(),
		startCmd,
		stopCmd,
		restartCmd,
		resetCmd,
	)

	return c
}
