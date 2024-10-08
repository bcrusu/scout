package storage

import (
	"time"

	"github.com/bcrusu/scout/internal/errors"
)

func (f *FSM) applyUpdateServerStatus(_ time.Time, cmd *UpdateServerStatus) (*UpdateResult, error) {
	if cmd.IfMatch != 0 && cmd.IfMatch != f.servers.StatusVersion {
		return nil, errors.FailedPrecondition
	}

	changed := false

	for id, update := range cmd.Items {
		if _, ok := f.servers.Items[id]; !ok {
			continue
		}
		status := f.servers.Status[id]

		status.Version++
		status.LastSeen = update.LastSeen
		status.LastAddress = update.LastAddress
		changed = true
	}

	if changed {
		f.servers.StatusVersion++
	}

	return &UpdateResult{
		NewVersion: f.servers.StatusVersion,
	}, nil
}
