package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newServiceLogsCmd() *cobra.Command {
	out := os.Stdout
	colorErr := 31 // red
	colors := []int{
		32, // green
		36, // cyan
		35, // magenta
		33, // yellow
		34, // blue
	}

	getLogs := func(ctx context.Context, nodeIp string) ([]byte, error) {
		client, err := agent.NewClient(nodeIp)
		if err != nil {
			return nil, err
		}
		defer client.Close()

		resp, err := client.GetLogs(ctx, nil)
		if err != nil {
			return nil, err
		}

		return resp.Data, nil
	}

	printErr := func(node *nodes.Node, format string, args ...any) {
		fmt.Fprintf(out, "\x1b[%d;1m", colorErr)
		fmt.Fprintf(out, "%s | ", node.Id)
		fmt.Fprintf(out, "\x1b[0;%dm", colorErr)
		fmt.Fprintf(out, format, args...)
		fmt.Fprintln(out, "\x1b[0m")
	}

	printLogs := func(ctx context.Context, node *nodes.Node, color int) {
		if node.State != nodes.NodeState_Running {
			printErr(node, "Node is not running")
			return
		}

		logs, err := getLogs(ctx, node.Ip)
		if err != nil {
			printErr(node, "Get logs failed. Error %s", err)
			return
		}

		buf := bytes.NewBuffer(logs)
		fmt.Fprintf(out, "\x1b[%dm", color)

		for {
			line, err := buf.ReadBytes('\n')
			if err != nil {
				break
			} else if len(line) != 0 {
				fmt.Fprintf(out, "%s | ", node.Id)
				out.Write(line)
			}
		}

		fmt.Fprint(out, "\x1b[0m")
	}

	c := &cobra.Command{
		Use:     "logs [ID]...",
		Aliases: []string{"log"},
		Short:   "Print service log entries for all or only the specified nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			client, err := newNodesClient(c)
			if err != nil {
				return err
			}
			defer client.Close()

			nodez, err := client.GetNodes(c.Context(), nil)
			if err != nil {
				return err
			}

			set := utils.MakeSet(args)
			colorIdx := 0

			for _, node := range nodez.Nodes {
				if len(set) > 0 && !set[node.Id] {
					continue
				}

				color := colors[colorIdx]
				colorIdx = (colorIdx + 1) % len(colors)
				printLogs(c.Context(), node, color)
			}

			return nil
		},
	}

	return c
}
