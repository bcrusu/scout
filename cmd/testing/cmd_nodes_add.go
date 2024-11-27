package main

import (
	"github.com/bcrusu/scout/cmd/testing/nodes"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/spf13/cobra"
)

func newNodesAddCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "add",
		Short: "Add nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			config, err := GetNodesConfig(c)
			if err != nil {
				return err
			}

			count, err := c.Flags().GetInt("count")
			if err != nil {
				return err
			} else if count < 1 || count > 10 {
				return errors.Error("invalid count value")
			}

			return nodes.AddNodes(config, count)
		},
	}

	c.PersistentFlags().Int("count", 1, "Nodes to add.")

	return c
}
