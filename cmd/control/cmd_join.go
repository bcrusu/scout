package main

import (
	"github.com/bcrusu/graph/internal/control/server"
	"github.com/bcrusu/graph/internal/control/server/config"
	"github.com/bcrusu/graph/internal/errors"
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

			if config.Get().Register == nil {
				return errors.Error("missing register config")
			}

			s := server.NewServer(server.DoRegister)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
