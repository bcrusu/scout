package main

import (
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newAgentCmd() *cobra.Command {
	return &cobra.Command{
		Use:           "agent",
		Short:         "Agent running inside the Firecracker microVM test node.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			log := logging.New("agent")
			server := agent.NewServer()
			return utils.LifecycleRun(c.Context(), log, server)
		},
	}
}
