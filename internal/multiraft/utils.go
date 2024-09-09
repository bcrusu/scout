package multiraft

import "github.com/hashicorp/raft"

func findServerForIdAndAddress(servers []raft.Server, id raft.ServerID, address raft.ServerAddress) (raft.Server, bool) {
	for _, s := range servers {
		if s.ID == id && s.Address == address {
			return s, true
		}
	}

	return raft.Server{}, false
}

func findServerForId(servers []raft.Server, id raft.ServerID) (raft.Server, bool) {
	for _, s := range servers {
		if s.ID == id {
			return s, true
		}
	}

	return raft.Server{}, false
}
