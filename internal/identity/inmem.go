package identity

import (
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/tracing"
)

type inmem struct {
	id *Identity
}

// NewInmem returns an in-memory identity store.
func NewInmem() Store {
	return &inmem{}
}

func (s *inmem) IsEmpty() bool {
	return s.id == nil
}

func (s *inmem) Set(id Identity) error {
	if s.id != nil {
		return errors.Error("identity was already set")
	}

	s.id = &id
	tracing.SetServerName(id.ServerName)
	return nil
}

func (s *inmem) Get() (Identity, bool) {
	if s.id == nil {
		return Identity{}, false
	}

	return *s.id, true
}
