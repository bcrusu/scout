package main

import (
	"github.com/bcrusu/graph/internal/control/server"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/spf13/cobra"
)

func newBootstrapCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "bootstrap",
		Aliases: []string{"b"},
		Short:   "Bootstraps a new cluster.",
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.WithComponent("cmd_bootstrap")
			config, err := getServerConfig(c)
			if err != nil {
				return err
			}

			peers, err := c.Flags().GetStringSlice("peers")
			if err != nil {
				return err
			}

			bconfig := server.BootstrapConfig{
				LocalAddress: config.Server.BindAddress,
				Peers:        peers,
			}

			s := server.NewServerForBootstrap(config, bconfig)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	c.Flags().StringSlice("peers", nil, "Initial server list. It must be identical for all bootstrapped servers. If not included, the local server will be appended.")

	return c
}
