package storage

import (
	"github.com/bcrusu/graph/internal/logging"
)

func (s *Server) AddToLog(log logging.Logger) logging.Logger {
	return log.With(
		"server_id", s.Id,
		"server_name", s.Name,
		"server_type", s.Type,
	)
}

func (s *Servers) ByID(id uint64) *Server {
	return s.Items[id]
}

func (s *Servers) ByType(stype ServerType) map[uint64]*Server {
	result := map[uint64]*Server{}

	for id, s := range s.Items {
		if s.Type == stype {
			result[id] = s
		}
	}

	return result
}
