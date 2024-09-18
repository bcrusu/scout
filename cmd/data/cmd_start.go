package main

import (
	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/data/server"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/spf13/cobra"
)

func newStartCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "start",
		Aliases: []string{"s"},
		Short:   "Starts from existing configuration on disk.",
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.WithComponent("cmd_start")
			config, err := cmd.GetConfig(c)
			if err != nil {
				return err
			}

			s := server.NewServer(serverConfig(config))
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
