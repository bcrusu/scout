package storage

import (
	"time"
)

// UpdateServers command will be applied in an async fire-and-forget manner and
// current state could have changed since last check.
func (f *FSM) applyUpdateServers(_ time.Time, cmd *UpdateServers) (*emptyResult, error) {
	f.servers.Version++

	for _, update := range cmd.Servers {
		server, ok := f.servers.Items[update.Id]

		// if the server was removed or updated with a newer state, skip it.
		if !ok || server.LastSeen.AsTime().After(update.LastSeen.AsTime()) {
			continue
		}

		server.Version++
		server.LastSeen = update.LastSeen
		server.LastAddress = update.LastAddress
	}

	return &emptyResult{}, nil
}
