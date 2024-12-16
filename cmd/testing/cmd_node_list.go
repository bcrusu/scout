package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func newNodeListCmd() *cobra.Command {
	type status struct {
		node *nodes.Node
		s    *agent.Status
		err  error
	}

	getStatus := func(ctx context.Context, node *nodes.Node) status {
		if node.State != nodes.NodeState_Running {
			return status{node: node}
		}

		client, err := agent.NewClient(node.Ip)
		if err != nil {
			log.WithError(err).Debug("Failed to create agent client.", "node", node.Id)
			return status{node: node, err: err}
		}
		defer client.Close()

		s, err := client.GetStatus(ctx, nil)
		if err != nil {
			log.WithError(err).Debug("Failed to get agent status.", "node", node.Id)
		}

		return status{node: node, s: s, err: err}
	}

	// invokes nodes in parallel to minimize time offset
	getStatusAll := func(ctx context.Context, all []*nodes.Node) map[string]status {
		statusCh := make(chan status)
		invoke := func(node *nodes.Node) {
			statusCh <- getStatus(ctx, node)
		}

		for _, node := range all {
			go invoke(node)
		}

		result := map[string]status{}
		for range len(all) {
			s := <-statusCh
			result[s.node.Id] = s
		}

		return result
	}

	getStatusValues := func(status status) (string, string, time.Time) {
		if status.node.State != nodes.NodeState_Running {
			return "-", "-", time.Time{}
		} else if status.err != nil {
			return "Error", "-", time.Time{}
		}

		service := "-"

		if status.s.ServiceType != agent.ServiceType_None {
			service = status.s.ServiceType.String()

			if status.s.ServiceActive {
				service += " (active)"
			}
		}

		return "OK", service, status.s.Time.AsTime()
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

			statusMap := getStatusAll(c.Context(), resp.Nodes)

			rows := make([][]string, len(resp.Nodes))
			var minTime time.Time
			var maxTime time.Time

			for i, node := range resp.Nodes {
				pid := "-"
				if node.Pid != 0 {
					pid = strconv.Itoa(int(node.Pid))
				}

				ip := "-"
				if node.Ip != "" {
					ip = node.Ip
				}

				agent, service, nodeTime := getStatusValues(statusMap[node.Id])

				timeFmt := "-"
				if !nodeTime.IsZero() {
					timeFmt = nodeTime.Format(utils.RFC3339Milli)

					if minTime.IsZero() || nodeTime.Before(minTime) {
						minTime = nodeTime
					}
					if maxTime.IsZero() || nodeTime.After(maxTime) {
						maxTime = nodeTime
					}
				}

				rows[i] = []string{
					node.Id,
					node.State.String(),
					pid,
					ip,
					agent,
					service,
					timeFmt,
				}
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Node", "State", "PID", "IP", "Agent", "Service", "Time"})
			table.AppendBulk(rows)
			table.Render()

			fmt.Printf("Max time offset: %s\n", maxTime.Sub(minTime))

			return nil
		},
	}
}
