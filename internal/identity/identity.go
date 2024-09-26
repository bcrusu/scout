package identity

import (
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/tracing"
	"github.com/google/uuid"
)

var (
	// TODO: implement on-disk persistence
	token    = uuid.New().String()
	identity *Identity
	log      = logging.WithComponent("identity").NoContext()
)

// Identity is the name of the machine.
type Identity struct {
	ClusterName string
	ServerID    uint64
	ServerName  string
}

// IdentityStore provides a way for servers to persist their identity bits.
// Once provided with an identity by the control plane, a server is expected
// to carry this information until the bitter end.
type IdentityStore interface {
	// Signals an empty store.
	IsEmpty() bool

	// Generates and returns the unique token used by servers to register
	// with the control plane. Further calls will return the same value.
	Token() string

	// Persists the identity received from the control plane. Further calls
	// will return error. Stored state is immutable.
	Set(Identity) error

	// Returns the stored state.
	Get() *Identity
}

type identityStore struct {
}

// NewStore returns an IdentityStore instance.
func NewStore(dataDir string) (IdentityStore, error) {

	// tracing.SetServerName(s.identity.ServerName)

	return &identityStore{}, nil
}

// Signals an empty store.
func (s *identityStore) IsEmpty() bool {
	return identity == nil
}

// Generates and returns the unique token used by servers to register
// with the control plane. Further calls will return the same value.
func (s *identityStore) Token() string {
	return token
}

// Persists the identity received from the control plane. Further calls
// will return error. Stored state is immutable.
func (s *identityStore) Set(i Identity) error {
	if identity != nil {
		return errors.Error("identity was already set")
	}

	identity = &i
	tracing.SetServerName(identity.ServerName)
	log.Info("Stored identity.", "cluster", i.ClusterName, "server_id", i.ServerID, "server_name", i.ServerName)
	return nil
}

// Returns the stored state.
func (s *identityStore) Get() *Identity {
	if identity == nil {
		return nil
	}
	return identity
}
