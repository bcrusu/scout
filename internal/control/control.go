package control

// IsHeartbeat returns true for empty messages which, by convention, are treated as heartbeats.
func (s *SessionIn) IsHeartbeat() bool {
	return s.ClusterName == "" && s.ServerId == 0 && s.Address == "" && s.Payload == nil
}

// ServerType infers the ServerType from payload type.
func (s *SessionIn) ServerType() ServerType {
	if s == nil {
		return ServerType_Unknown
	}

	switch s.Payload.(type) {
	case *SessionIn_Control:
		return ServerType_Control
	case *SessionIn_Data:
		return ServerType_Data
	case *SessionIn_Api:
		return ServerType_Api
	default:
		return ServerType_Unknown
	}
}
