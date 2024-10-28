package main

import (
	"github.com/bcrusu/scout/internal/control/server"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newStartCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "start",
		Aliases: []string{"s"},
		Short:   "Starts from existing configuration on disk.",
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.New("cmd_start")
			s := server.NewServer(server.DoStart)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
