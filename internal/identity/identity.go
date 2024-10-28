package identity

import "github.com/bcrusu/scout/internal/logging"

var (
	log = logging.New("identity").NoContext()
)

// Identity is the name of the machine.
type Identity struct {
	ClusterName string `json:"clusterName"`
	ServerID    uint64 `json:"serverId"`
	ServerName  string `json:"serverName"`
}

// Store provides a way for servers to persist their identity bits.
// Once provided with an identity by the control plane, a server is expected
// to carry this information until the bitter end.
type Store interface {
	// Signals an empty store.
	IsEmpty() bool

	// Persists the identity received from the control plane. Further calls
	// will return error. Stored state is immutable.
	Set(Identity) error

	// Returns the stored state.
	Get() (Identity, bool)
}
