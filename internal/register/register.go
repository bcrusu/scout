package register

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

	log = logging.WithComponent("register")
)

type Params struct {
	ServerType  control.ServerType
	ClusterName string
	BindAddress string
	DataDir     string
	Discovery   discovery.Discovery
}

// Register provides reusable functionality for registering a node in the cluster.
func Register(ctx context.Context, params Params) error {
	idStore, err := identity.NewStore(params.DataDir)
	if err != nil {
		return err
	}

	if id, ok := idStore.Get(); ok {
		return errors.Errorf("cannot register, already part of cluster %s", id.ClusterName)
	}

	res, err := registerWithRetry(ctx, params, idStore.Token())
	if err != nil {
		return err
	}

	id := identity.Identity{
		ClusterName: params.ClusterName,
		ServerID:    res.ServerId,
		ServerName:  res.ServerName,
	}

	if err := idStore.Set(id); err != nil {
		return err
	}

	return nil
}

func registerWithRetry(ctx context.Context, params Params, token string) (*control.RegisterResponse, error) {
	opts := []client.Option{
		client.WithTarget(discovery.NewTarget(params.ClusterName, params.Discovery)),
	}

	client := client.New(opts...)

	if err := client.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to start control client")
	}
	defer client.Stop()

	req := &control.RegisterRequest{
		ClusterName: params.ClusterName,
		Token:       token,
		Address:     params.BindAddress,
		Type:        params.ServerType,
	}

	return utils.RetryR(ctx, registerBackoff, func() (*control.RegisterResponse, error) {
		resp, err := client.Register(ctx, req)
		if err != nil {
			log.WithError(err).Error(ctx, "Register failed. Retrying...")
		} else {
			log.Info(ctx, "Registered with success.", "server_id", resp.ServerId, "server_name", resp.ServerName)
		}
		return resp, err
	})
}
