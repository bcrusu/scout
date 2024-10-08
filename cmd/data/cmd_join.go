package main

import (
	"github.com/bcrusu/scout/internal/data/server"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newJoinCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "join",
		Aliases: []string{"j"},
		Short:   "Joins an existing cluster.",
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.WithComponent("cmd_join")
			s := server.NewServer(server.DoRegister)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
