package main

import (
	"os"
	"strconv"

	"github.com/bcrusu/scout/cmd/testing/nodes"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func newNodesListCmd() *cobra.Command {
	c := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			config, err := GetNodesConfig(c)
			if err != nil {
				return err
			}

			nodes, err := nodes.ListNodes(config.NodesDir)
			if err != nil {
				return err
			} else if len(nodes) <= 0 {
				return nil
			}

			rows := make([][]string, len(nodes))

			for i, node := range nodes {
				pidStr := "-"
				if pid := node.GetPID(); pid > 0 {
					pidStr = strconv.FormatInt(int64(pid), 10)
				}

				ip := "-"
				if x := node.GetIP(); x != "" {
					ip = x
				}

				rows[i] = []string{
					node.ID,
					node.GetState(),
					pidStr,
					ip,
				}
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Node", "State", "PID", "IP"})
			table.AppendBulk(rows)
			table.Render()

			return nil
		},
	}

	return c
}
