package main

import (
	"github.com/bcrusu/scout/internal/control/server"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/errors"
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
			log := logging.New("cmd_join")

			if config.Get().Register == nil {
				return errors.Error("missing register config")
			}

			s := server.NewServer(server.DoRegister)
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	return c
}
