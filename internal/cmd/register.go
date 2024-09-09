package cmd

import (
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/client"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/spf13/cobra"
)

var (
	registerBackoff = &utils.Backoff{
		MinDelay: 2 * time.Second,
		MaxDelay: 30 * time.Second,
	}
)

// Register provides reusable functionality for registering a node in the cluster.
// It expects the input command to define the flags in the adjacent params.go file.
func Register(c *cobra.Command, log logging.Logger) error {
	idStore, err := identity.NewStore()
	if err != nil {
		return err
	}

	if id, ok := idStore.Get(); ok {
		return errors.Errorf("cannot register, already part of cluster %s", id.ClusterName)
	}

	id, err := registerWithRetry(c, log, idStore.Token())
	if err != nil {
		return err
	}

	if err := idStore.Set(id); err != nil {
		return err
	}

	return nil
}

func registerWithRetry(cmd *cobra.Command, log logging.Logger, token string) (id identity.Identity, err error) {
	ctx := cmd.Context()

	bindAddress, err := cmd.Flags().GetString("bind-address")
	if err != nil {
		return id, err
	}

	clusterName, err := cmd.Flags().GetString("cluster-name") // TODO: better param validation
	if err != nil {
		return id, err
	}

	client, err := createControlClient(cmd, clusterName)
	if err != nil {
		return id, errors.Wrap(err, "failed to create control plane client")
	}

	if err := client.Start(ctx); err != nil {
		return id, errors.Wrap(err, "failed to start control client")
	}
	defer client.Stop(ctx)

	req := &control.RegisterRequest{
		ClusterName: clusterName,
		Token:       token,
		Address:     bindAddress,
		Payload: &control.RegisterRequest_Control{
			Control: &control.RegisterRequest_ControlReq{},
		},
	}

	res, err := utils.RetryR(ctx, registerBackoff, func() (*control.RegisterResponse, error) {
		resp, err := client.Register(ctx, req)
		if err != nil {
			log.WithError(err).Error(ctx, "Register failed. Retrying...")
		} else {
			log.Info(ctx, "Registered with success.")
		}
		return resp, err
	})
	if err != nil {
		return id, err
	}

	return identity.Identity{
		ClusterName: clusterName,
		ID:          res.ServerId,
		Name:        res.ServerName,
	}, nil
}

func createControlClient(cmd *cobra.Command, clusterName string) (client.ControlClient, error) {
	d, err := cmd.Flags().GetString("discovery")
	if err != nil {
		return nil, err
	}

	var discoveryTarget discovery.DiscoveryTarget

	switch d {
	case "static":
		peers, err := cmd.Flags().GetStringSlice("peers")
		if err != nil {
			return nil, err
		}
		discoveryTarget = discovery.Static(peers...)
	case "dns":
		target, err := cmd.Flags().GetString("target")
		if err != nil {
			return nil, err
		}
		discoveryTarget = discovery.DNS(target)
	default:
		return nil, errors.Errorf("unknown discovery type %q", d)
	}

	opts := []client.Option{
		client.WithTarget(discovery.Target{
			ClusterName: clusterName,
			Discovery:   discoveryTarget,
		}),
	}

	return client.NewClient(opts...), nil
}
