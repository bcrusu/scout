package main

import (
	"context"
	"os"
	"strconv"

	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func newNodeListCmd() *cobra.Command {
	getAgentStatus := func(ctx context.Context, node *nodes.Node) (string, string) {
		if node.State != nodes.NodeState_Running {
			return "-", "-"
		}

		client, err := agent.NewClient(node.Ip)
		if err != nil {
			log.WithError(err).Debug("Failed to create agent client.", "node", node.Id)
			return "Error", "-"
		}
		defer client.Close()

		status, err := client.GetStatus(ctx, nil)
		if err != nil {
			log.WithError(err).Debug("Failed to get agent status.", "node", node.Id)
			return "Error", "-"
		}

		service := "-"

		if status.ServiceType != agent.ServiceType_None {
			service = status.ServiceType.String()

			if status.ServiceActive {
				service += " (active)"
			}
		}

		return "OK", service
	}

	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			client, err := newNodesClient(c)
			if err != nil {
				return err
			}
			defer client.Close()

			resp, err := client.GetNodes(c.Context(), nil)
			if err != nil {
				return err
			} else if len(resp.Nodes) == 0 {
				return nil
			}

			rows := make([][]string, len(resp.Nodes))

			for i, node := range resp.Nodes {
				pid := "-"
				if node.Pid != 0 {
					pid = strconv.Itoa(int(node.Pid))
				}

				ip := "-"
				if node.Ip != "" {
					ip = node.Ip
				}

				agent, service := getAgentStatus(c.Context(), node)

				rows[i] = []string{
					node.Id,
					node.State.String(),
					pid,
					ip,
					agent,
					service,
				}
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Node", "State", "PID", "IP", "Agent", "Service"})
			table.AppendBulk(rows)
			table.Render()

			return nil
		},
	}
}
