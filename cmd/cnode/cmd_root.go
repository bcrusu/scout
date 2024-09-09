package main

import (
	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/control/server"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "cnode",
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

func getServerConfig(c *cobra.Command) (server.Config, error) {
	serverConfig, err0 := cmd.GetServerConfig(c)

	clusterName, err1 := c.Flags().GetString("cluster-name")
	if err1 == nil && clusterName == "" {
		err1 = errors.Error("cluster-name cannot be empty")
	}

	dataDir, err2 := c.Flags().GetString("data-dir")
	if err2 == nil && dataDir == "" {
		err2 = errors.Error("data-dir cannot be empty")
	}

	err := errors.Join(err0, err1, err2)
	if err != nil {
		return server.Config{}, err
	}

	return server.Config{
		Server:      serverConfig,
		ClusterName: clusterName,
		DataDir:     dataDir,
	}, nil
}
