package main

import (
	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/rpc"
	"github.com/bcrusu/scout/internal/rpc/routing"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

var (
	rpcConn       *rpc.Conn
	controlClient control.ServiceClient
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "scout",
		Short:         "Interact with the cluster.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			conn, err := newConn(c)
			if err != nil {
				return err
			}

			rpcConn = conn
			controlClient = control.NewServiceClient(conn)
			return nil
		},
		PersistentPostRun: func(c *cobra.Command, args []string) {
			if rpcConn != nil {
				rpcConn.Close()
			}
		},
	}

	root.PersistentFlags().StringP("server", "s", "", "Servers.")

	root.AddCommand(
		newGetCmd(),
	)

	return root
}

func newConn(c *cobra.Command) (*rpc.Conn, error) {
	server, err := c.Flags().GetString("server")
	if err != nil {
		return nil, err
	} else if server == "" {
		return nil, errors.Error("missing server flag")
	}

	server = utils.EnsureAddressPort(server, rpc.DefaultPort)

	target := routing.FormatTargetStatic(server)
	conn := rpc.NewAdminConn(target)
	if err := conn.Start(c.Context()); err != nil {
		return nil, err
	}

	return conn, nil
}
