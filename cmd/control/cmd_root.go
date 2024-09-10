package main

import (
	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/control/server"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "control",
		Short:         "Graph control plane server.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRun: func(c *cobra.Command, args []string) {
			cmd.SetLogLevel(c)
		},
	}

	cmd.AddAllParameters(c)

	c.AddCommand(
		newBootstrapCmd(),
		newJoinCmd(),
		newStartCmd(),
	)

	return c
}

func serverConfig(c cmd.Config) server.Config {
	return server.Config{
		Server:      c.Server,
		ClusterName: c.ClusterName,
		DataDir:     c.DataDir,
	}
}
