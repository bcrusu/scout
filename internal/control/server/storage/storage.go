package storage

import "github.com/bcrusu/graph/internal/logging"

func (s *Server) AddToLog(log logging.Logger) logging.Logger {
	return log.With(
		"server_id", s.Id,
		"server_name", s.Name,
		"server_type", s.Type,
	)
}
