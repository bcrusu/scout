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
		Short:   "Bootstraps a new cluster. Must be executed using the same params on all bootstrapped servers.",
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.WithComponent("cmd_bootstrap")
			config, err := cmd.GetConfig(c)
			if err != nil {
				return err
			}

			peers, err1 := c.Flags().GetStringSlice("initial-servers")
			partitionCount, err2 := c.Flags().GetUint32("partition-count")
			if err := errors.Join(err1, err2); err != nil {
				return err
			}
			for _, a := range peers {
				if !storage.IsValidAddress(a) {
					return errors.Error("initial-servers contains invalid address")
				}
			}

			bconfig := server.BootstrapConfig{
				LocalAddress:   config.Server.BindAddress,
				Peers:          peers,
				PartitionCount: partitionCount,
			}

			s := server.NewServerForBootstrap(serverConfig(config), bconfig)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	c.Flags().StringSlice("initial-servers", nil, "Initial server list. If not included, the local server will be appended.")
	c.Flags().Uint32("partition-count", 16, "The number of partitions in the data storage cluster.")

	return c
}
