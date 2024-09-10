package main

import (
	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/data/server"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "data",
		Short:         "Graph data storage server.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRun: func(c *cobra.Command, args []string) {
			cmd.SetLogLevel(c)
		},
	}

	cmd.AddAllParameters(c)

	c.AddCommand(
		newJoinCmd(),
		newStartCmd(),
	)

	return c
}

func serverConfig(c cmd.Config) server.Config {
	return server.Config{
		Server:      c.Server,
		ClusterName: c.ClusterName,
		Discovery:   c.Discovery,
		DataDir:     c.DataDir,
	}
}
