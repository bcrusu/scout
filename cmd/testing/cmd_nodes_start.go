package main

import (
	"github.com/bcrusu/scout/cmd/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newNodesStartCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "start [ID]...",
		Short: "Start all or only the the specified nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			config, err := GetNodesConfig(c)
			if err != nil {
				return err
			}

			nodes, err := nodes.ListNodes(config.NodesDir)
			if err != nil {
				return err
			} else if len(nodes) == 0 {
				return nil
			}

			toStart := utils.MakeSet(args)

			for _, node := range nodes {
				if len(toStart) > 0 && !toStart[node.ID] {
					continue
				}

				if err := node.Start(config); err != nil {
					log.WithError(err).Error("Failed to start.", "node", node.ID)
				}
			}

			return nil
		},
	}

	return c
}
