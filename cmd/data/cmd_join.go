package main

import (
	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/data/server"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/register"
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
			config, err := getConfig(c)
			if err != nil {
				return err
			}

			params := register.Params{
				ServerType:  control.ServerType_Data,
				ClusterName: config.ClusterName,
				BindAddress: config.Server.BindAddress,
				DataDir:     config.DataDir,
				Discovery:   config.Discovery,
			}

			if err := register.Register(c.Context(), params); err != nil {
				return err
			}

			s := server.NewServer(config)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
