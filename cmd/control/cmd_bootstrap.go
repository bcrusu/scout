package main

import (
	"github.com/bcrusu/scout/internal/control/server"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newBootstrapCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "bootstrap",
		Aliases: []string{"b"},
		Short:   "Bootstraps a new cluster. Must be executed using the same params on all bootstrapped servers.",
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.WithComponent("cmd_bootstrap")

			if config.Get().Bootstrap == nil {
				return errors.Error("missing bootstrap config")
			}

			s := server.NewServer(server.DoBootstrap)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
