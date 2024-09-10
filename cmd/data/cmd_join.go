package main

import (
	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data/server"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/spf13/cobra"
)

func newJoinCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "join",
		Aliases: []string{"j"},
		Short:   "Joins an existing cluster.",
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.WithComponent("cmd_join")
			config, err := cmd.GetConfig(c)
			if err != nil {
				return err
			}

			if err := cmd.Register(c.Context(), log, config, control.RegisterRequest_Data); err != nil {
				return err
			}

			s := server.NewServer(serverConfig(config))
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
