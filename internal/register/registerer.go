package register

import (
	"context"
	"time"

	"github.com/bcrusu/graph/internal/control"
	"github.com/bcrusu/graph/internal/control/client"
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
}

// Registerer is used to register a server in the cluster.
type Registerer struct {
	idStore identity.IdentityStore
	client  client.ControlClient
}

// NewRegisterer returns a new Registerer.
func NewRegisterer(idStore identity.IdentityStore, client client.ControlClient) *Registerer {
	return &Registerer{
		idStore: idStore,
		client:  client,
	}
}

func (r *Registerer) Register(ctx context.Context, params Params) (*identity.Identity, error) {
	if id := r.idStore.Get(); id != nil {
		return nil, errors.Errorf("cannot register, already part of cluster %s", id.ClusterName)
	}

	req := &control.RegisterRequest{
		ClusterName: params.ClusterName,
		Token:       r.idStore.Token(),
		Address:     params.BindAddress,
		Type:        params.ServerType,
	}

	res, err := utils.RetryForeverR(ctx, registerBackoff, func() (*control.RegisterResponse, error) {
		resp, err := r.client.Register(ctx, req)
		if err != nil {
			log.WithError(err).Error(ctx, "Register failed. Retrying...")
		} else {
			log.Info(ctx, "Registered with success.", "server_id", resp.ServerId, "server_name", resp.ServerName)
		}
		return resp, err
	})

	if err != nil {
		return nil, err
	}

	id := identity.Identity{
		ClusterName: params.ClusterName,
		ServerID:    res.ServerId,
		ServerName:  res.ServerName,
	}

	if err := r.idStore.Set(id); err != nil {
		return nil, err
	}

	return &id, nil
}
