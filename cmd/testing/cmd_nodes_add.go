package main

import (
	"strconv"

	"github.com/bcrusu/scout/cmd/testing/nodes"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/spf13/cobra"
)

func newNodesAddCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "add COUNT",
		Short: "Add nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			config, err := GetNodesConfig(c)
			if err != nil {
				return err
			}

			if len(args) != 1 {
				return errors.Error("expected a single count arg")
			}

			count, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return errors.Error("could not parse count arg")
			}

			if count < 1 || count > 10 {
				return errors.Error("invalid count value")
			}

			return nodes.AddNodes(config, int(count))
		},
	}

	return c
}
