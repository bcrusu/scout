package main

import (
	"github.com/spf13/cobra"
)

func newGetCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "get",
		Aliases: []string{"g"},
		Short:   "Get cluster resources.",
	}

	c.AddCommand(
		newGetServersCmd(),
		newGetPartitionsCmd(),
		newGetReplicasCmd(),
	)

	return c
}
