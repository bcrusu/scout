package storage

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
