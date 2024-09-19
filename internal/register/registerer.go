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
	_   utils.Lifecycle = (*Registerer)(nil)
	log                 = logging.WithComponent("register")
)

type Params struct {
	ServerType  control.ServerType
	ClusterName string
	BindAddress string
}

// Registerer is used to register a server in the cluster.
type Registerer struct {
	idStore    identity.IdentityStore
	client     client.ControlClient
	params     Params
	cancelFunc context.CancelFunc
}

// NewRegisterer returns a new Registerer.
func NewRegisterer(idStore identity.IdentityStore, client client.ControlClient, params Params) *Registerer {
	return &Registerer{
		idStore: idStore,
		client:  client,
		params:  params,
	}
}

// Register provides reusable functionality for registering a node in the cluster.
func (r *Registerer) Start(ctx context.Context) error {
	if id, ok := r.idStore.Get(); ok {
		return errors.Errorf("cannot register, already part of cluster %s", id.ClusterName)
	}

	cancelCtx, cancelFunc := context.WithCancel(ctx)
	r.cancelFunc = cancelFunc
	return r.registerWithRetry(cancelCtx)
}

func (r *Registerer) Stop() {
	r.cancelFunc()
}

func (r *Registerer) registerWithRetry(ctx context.Context) error {
	req := &control.RegisterRequest{
		ClusterName: r.params.ClusterName,
		Token:       r.idStore.Token(),
		Address:     r.params.BindAddress,
		Type:        r.params.ServerType,
	}

	res, err := utils.RetryR(ctx, registerBackoff, func() (*control.RegisterResponse, error) {
		resp, err := r.client.Register(ctx, req)
		if err != nil {
			log.WithError(err).Error(ctx, "Register failed. Retrying...")
		} else {
			log.Info(ctx, "Registered with success.", "server_id", resp.ServerId, "server_name", resp.ServerName)
		}
		return resp, err
	})

	if err != nil {
		return err
	}

	id := identity.Identity{
		ClusterName: r.params.ClusterName,
		ServerID:    res.ServerId,
		ServerName:  res.ServerName,
	}

	return r.idStore.Set(id)
}
