package main

import (
	"github.com/bcrusu/scout/cmd/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newNodesRemoveCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "rm [ID]...",
		Short: "Remove all or only the specified nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			config, err := GetNodesConfig(c)
			if err != nil {
				return err
			}

			nodeSlice, err := nodes.ListNodes(config.NodesDir)
			if err != nil {
				return err
			} else if len(nodeSlice) == 0 {
				return nil
			}

			toRemove := utils.MakeSet(args)

			for _, node := range nodeSlice {
				if len(toRemove) > 0 && !toRemove[node.ID] {
					continue
				}

				if err := nodes.RemoveNode(config, node.ID); err != nil {
					log.WithError(err).Error("Failed to remove.", "node", node.ID)
				}
			}

			return nil
		},
	}

	return c
}
