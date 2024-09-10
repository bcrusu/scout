package main

import (
	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/control/server"
	"github.com/bcrusu/graph/internal/control/server/storage"
	"github.com/bcrusu/graph/internal/errors"
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
			config, err := cmd.GetConfig(c)
			if err != nil {
				return err
			}

			peers, err := c.Flags().GetStringSlice("initial-servers")
			if err != nil {
				return err
			}
			for _, a := range peers {
				if !storage.IsValidAddress(a) {
					return errors.Error("initial-servers contains invalid address")
				}
			}

			bconfig := server.BootstrapConfig{
				LocalAddress: config.Server.BindAddress,
				Peers:        peers,
			}

			s := server.NewServerForBootstrap(serverConfig(config), bconfig)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	c.Flags().StringSlice("initial-servers", nil, "Initial server list. It must be identical for all bootstrapped servers. If not included, the local server will be appended.")

	return c
}
