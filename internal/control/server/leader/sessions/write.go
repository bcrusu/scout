package sessions

import (
	"context"

	"github.com/bcrusu/graph/internal/control/server/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (t *Tracker) writeUpdateServers(ctx context.Context, servers *storage.Servers, sessions sessionsByServer) {
	update := &storage.UpdateServers{
		Servers: make([]*storage.UpdateServers_Server, 0, len(sessions)),
	}

	for _, server := range servers.Items {
		sid := serverID(server.Id)
		sess := sessions[sid]
		if sess == nil {
			continue
		}

		// Skip the update only if address did not change which will not update
		// server last seen time in storage. This is fine because this field has,
		// for now, only informative value and can be fetched live from control
		// plane leader by admin/stats tooling. This could change in the future
		// if automated server removal/purge controller will be added.
		if sess.serverAddress == server.LastAddress {
			continue
		}

		update.Servers = append(update.Servers, &storage.UpdateServers_Server{
			Id:          server.Id,
			LastSeen:    timestamppb.New(sess.lastSeen),
			LastAddress: sess.serverAddress,
		})
	}

	if len(update.Servers) == 0 {
		return
	}

	if err := t.store.UpdateServersAsync(update); err != nil {
		logS.WithError(err).Error(ctx, "UpdateServersAsync failed")
	}
}
