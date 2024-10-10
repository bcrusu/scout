package multiraft

import "github.com/hashicorp/raft"

func findServerForId(servers []raft.Server, id raft.ServerID) (raft.Server, bool) {
	for _, s := range servers {
		if s.ID == id {
			return s, true
		}
	}

	return raft.Server{}, false
}
