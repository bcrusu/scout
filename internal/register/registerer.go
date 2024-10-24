package register

import (
	"context"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/control/client"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

var (
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
	backoff utils.Backoff
}

// NewRegisterer returns a new Registerer.
func NewRegisterer(idStore identity.IdentityStore, client client.ControlClient, backoff utils.Backoff) *Registerer {
	return &Registerer{
		idStore: idStore,
		client:  client,
		backoff: backoff,
	}
}

func (r *Registerer) Register(ctx context.Context, params Params) (*identity.Identity, error) {
	if id := r.idStore.Get(); id != nil {
		return nil, errors.Errorf("cannot register, already part of cluster %s", id.ClusterName)
	}

	req := &control.RegisterRequest{
		Token:   r.idStore.Token(),
		Address: params.BindAddress,
		Type:    params.ServerType,
	}

	res, err := utils.RetryForeverR(ctx, &r.backoff, func() (*control.RegisterResponse, error) {
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
