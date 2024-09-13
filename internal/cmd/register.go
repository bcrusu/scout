package cmd

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/client"
	"github.com/bcrusu/graph/internal/discovery"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/identity"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
)

var (
	registerBackoff = &utils.Backoff{
		MinDelay: 2 * time.Second,
		MaxDelay: 30 * time.Second,
	}
)

// Register provides reusable functionality for registering a node in the cluster.
func Register(ctx context.Context, log logging.Logger, config Config, serverType control.ServerType) error {
	idStore, err := identity.NewStore(config.DataDir)
	if err != nil {
		return err
	}

	if id, ok := idStore.Get(); ok {
		return errors.Errorf("cannot register, already part of cluster %s", id.ClusterName)
	}

	res, err := registerWithRetry(ctx, log, config, serverType, idStore.Token())
	if err != nil {
		return err
	}

	id := identity.Identity{
		ClusterName: config.ClusterName,
		ID:          res.ServerId,
		Name:        res.ServerName,
	}

	if err := idStore.Set(id); err != nil {
		return err
	}

	return nil
}

func registerWithRetry(ctx context.Context, log logging.Logger, config Config, serverType control.ServerType, token string) (*control.RegisterResponse, error) {
	opts := []client.Option{
		client.WithTarget(discovery.NewTarget(config.ClusterName, config.Discovery)),
	}

	client := client.New(opts...)

	if err := client.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to start control client")
	}
	defer client.Stop(ctx)

	req := &control.RegisterRequest{
		ClusterName: config.ClusterName,
		Token:       token,
		Address:     config.Server.BindAddress,
		Type:        serverType,
	}

	return utils.RetryR(ctx, registerBackoff, func() (*control.RegisterResponse, error) {
		resp, err := client.Register(ctx, req)
		if err != nil {
			log.WithError(err).Error(ctx, "Register failed. Retrying...")
		} else {
			log.Info(ctx, "Registered with success.")
		}
		return resp, err
	})
}
