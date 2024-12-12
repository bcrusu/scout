package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func newNodeCmd() *cobra.Command {
	type action func(context.Context, *nodes.Ids, ...grpc.CallOption) (*nodes.Status, error)
	type selector func(*nodes.Client) action

	run := func(c *cobra.Command, ids []string, selector selector) error {
		client, err := newNodesClient(c)
		if err != nil {
			return err
		}
		defer client.Close()

		req := &nodes.Ids{Ids: ids}
		action := selector(client)

		if status, err := action(c.Context(), req); err != nil {
			return err
		} else if status.FailureCount > 0 {
			log.Warnf("Operation competed with %d errors.", status.FailureCount)
		}

		return nil
	}

	startCmd := &cobra.Command{
		Use:   "start [ID]...",
		Short: "Start all or only the the specified nodes.",
		RunE: func(c *cobra.Command, ids []string) error {
			return run(c, ids, func(clt *nodes.Client) action { return clt.Start })
		},
	}

	stopCmd := &cobra.Command{
		Use:   "stop [ID]...",
		Short: "Stop all or only the the specified nodes.",
		RunE: func(c *cobra.Command, ids []string) error {
			return run(c, ids, func(clt *nodes.Client) action { return clt.Stop })
		},
	}

	resetCmd := &cobra.Command{
		Use:   "reset [ID]...",
		Short: "Reset state for all or only the specified nodes.",
		RunE: func(c *cobra.Command, ids []string) error {
			return run(c, ids, func(clt *nodes.Client) action { return clt.Reset })
		},
	}

	removeCmd := &cobra.Command{
		Use:     "remove [ID]...",
		Aliases: []string{"rm"},
		Short:   "Remove all or only the specified nodes.",
		RunE: func(c *cobra.Command, ids []string) error {
			return run(c, ids, func(clt *nodes.Client) action { return clt.Remove })
		},
	}

	createCmd := &cobra.Command{
		Use:   "create COUNT",
		Short: "Create nodes.",
		RunE: func(c *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.Error("expected a single count arg")
			}
			count, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return errors.Error("could not parse count arg")
			}

			client, err := newNodesClient(c)
			if err != nil {
				return err
			}
			defer client.Close()

			_, err = client.Create(c.Context(), &nodes.CreateRequest{Count: int32(count)})
			return err
		},
	}

	ipCmd := &cobra.Command{
		Use:   "ip ID",
		Short: "Get the IP address for the specified node.",
		RunE: func(c *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.Error("invalid node id arg")
			}

			client, err := newNodesClient(c)
			if err != nil {
				return err
			}
			defer client.Close()

			node, err := client.GetNode(c.Context(), &nodes.Id{Id: args[0]})
			if err != nil {
				return err
			}

			if node.Ip != "" {
				fmt.Print(node.Ip)
			} else {
				fmt.Print("NODE_NOT_RUNNING")
			}

			return nil
		},
	}

	c := &cobra.Command{
		Use:           "node",
		Aliases:       []string{"n"},
		Short:         "Firecracker microVM test nodes.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	c.PersistentFlags().String("work-dir", "", "Current dir if not specified.")

	c.AddCommand(
		newNodeDaemonCmd(),
		newNodeListCmd(),
		createCmd,
		startCmd,
		stopCmd,
		resetCmd,
		removeCmd,
		ipCmd,
	)

	return c
}
