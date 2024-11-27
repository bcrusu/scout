package main

import (
	"github.com/bcrusu/scout/cmd/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newNodesStopCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "stop",
		Short: "Stop the specified nodes.",
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

			toStop := utils.MakeSet(args)

			for _, node := range nodes {
				if len(toStop) > 0 && !toStop[node.ID] {
					continue
				}

				if err := node.Stop(); err != nil {
					log.WithError(err).Error("Failed to stop.", "node", node.ID)
				}
			}

			return nil
		},
	}

	return c
}
