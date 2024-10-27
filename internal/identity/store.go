package identity

import (
	"encoding/json"
	"os"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/tracing"
	"github.com/bcrusu/scout/internal/utils"
)

type store struct {
	filePath string
	id       *Identity
}

// NewStore returns a new on-disk persistent identity store instance.
func NewStore(filePath string) (Store, error) {
	exists, err := utils.PathExists(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to determine if the identity file exists.")
	}

	var id *Identity

	if exists {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read the identity file.")
		}

		id = &Identity{}
		if err := json.Unmarshal(data, id); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal the identity file.")
		}

		tracing.SetServerName(id.ServerName)
		log.Info("Loaded identity.", "cluster", id.ClusterName, "server_id", id.ServerID, "server_name", id.ServerName)
	}

	return &store{
		filePath: filePath,
		id:       id,
	}, nil
}

// Signals an empty store.
func (s *store) IsEmpty() bool {
	return s.id == nil
}

// Persists the identity received from the control plane. Further calls
// will return error. Stored state is immutable.
func (s *store) Set(id Identity) error {
	if s.id != nil {
		return errors.Error("identity was already set")
	}

	data, err := json.Marshal(id)
	if err != nil {
		return errors.Wrap(err, "failed to marshal the identity.")
	}

	if err := os.WriteFile(s.filePath, data, 0755); err != nil {
		return errors.Wrap(err, "failed to write the identity file.")
	}

	s.id = &id
	tracing.SetServerName(id.ServerName)

	log.Info("Stored identity.", "cluster", id.ClusterName, "server_id", id.ServerID, "server_name", id.ServerName)
	return nil
}

// Returns the stored identity.
func (s *store) Get() (Identity, bool) {
	if s.id == nil {
		return Identity{}, false
	}

	return *s.id, true
}
