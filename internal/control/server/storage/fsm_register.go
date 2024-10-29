package storage

import (
	"fmt"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// A token can be used to register only in this time window.
	// Later attempts should use a new token.
	// It is used to detect bad deployments where straggling
	// servers keep retrying ad infinitum. In the future, tokens
	// could contain their validity window.
	tokenValidityWindow = 2 * 24 * time.Hour
)

var (
	serverNamePrefix = map[ServerType]string{
		ServerType_Control: "control_",
		ServerType_Data:    "data_",
		ServerType_Api:     "api_",
	}
)

func (f *FSM) applyRegister(appendedAt time.Time, cmd *Register) (*RegisterResult, error) {
	// Trying to register using the same token: return previous server info only if token is still valid.
	if id, ok := f.servers.Tokens[cmd.Token]; ok {
		server := f.servers.Items[id]

		registeredAt := server.RegisteredAt.AsTime()
		validFrom := appendedAt.Add(-tokenValidityWindow)
		if registeredAt.Before(validFrom) {
			return nil, errors.ValidationError{Message: "Token expired."}
		}

		return &RegisterResult{
			ServerID:   id,
			ServerName: server.Name,
		}, nil
	}

	id := f.nextServerID()
	name := fmt.Sprintf("%s%d", serverNamePrefix[cmd.Type], id)

	f.servers.Items[id] = &Server{
		Id:           id,
		Name:         name,
		Type:         cmd.Type,
		RegisteredAt: timestamppb.New(appendedAt),
		LastSeen:     timestamppb.New(appendedAt),
		LastAddress:  cmd.Address,
	}

	f.servers.Tokens[cmd.Token] = id
	f.servers.RegisterVersion++
	f.servers.Version++

	return &RegisterResult{
		ServerID:   id,
		ServerName: name,
	}, nil
}
