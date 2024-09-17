package identity

import (
	"github.com/bcrusu/graph/internal/tracing"
	"github.com/google/uuid"
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
	Get() (Identity, bool)
}

type identityStore struct {
	token    string
	identity *Identity
}

// NewStore returns an IdentityStore instance.
func NewStore(dataDir string) (IdentityStore, error) {
	// TODO: implement on-disk persistence

	// tracing.SetServerName(s.identity.ServerName)

	return &identityStore{
		token: uuid.New().String(),
	}, nil
}

// Signals an empty store.
func (s *identityStore) IsEmpty() bool {
	return s.identity == nil
}

// Generates and returns the unique token used by servers to register
// with the control plane. Further calls will return the same value.
func (s *identityStore) Token() string {
	return s.token
}

// Persists the identity received from the control plane. Further calls
// will return error. Stored state is immutable.
func (s *identityStore) Set(i Identity) error {
	s.identity = &i
	tracing.SetServerName(s.identity.ServerName)
	return nil
}

// Returns the stored state.
func (s *identityStore) Get() (Identity, bool) {
	if s.identity == nil {
		return Identity{}, false
	}
	return *s.identity, true
}
