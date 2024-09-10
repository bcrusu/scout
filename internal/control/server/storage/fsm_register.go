package storage

import (
	"fmt"
	"time"

	"github.com/bcrusu/graph/internal/errors"
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
	// Trying to register using the same token. Will return previous member
	// info only if token is still valid.
	if id, ok := f.tokens[cmd.Token]; ok {
		server := f.servers.Items[id]

		firstSeen := server.FirstSeen.AsTime()
		validFrom := appendedAt.Add(-tokenValidityWindow)
		if firstSeen.Before(validFrom) {
			return nil, errors.ValidationError{Message: "Token expired."}
		}

		return &RegisterResult{
			ServerID:   id,
			ServerName: server.Name,
		}, nil
	}

	f.lastServerID++

	id := f.lastServerID
	name := fmt.Sprintf("%s%d", serverNamePrefix[cmd.Type], id)

	f.tokens[cmd.Token] = id

	f.servers.Version++
	f.servers.Items[id] = &Server{
		Version:     1,
		Id:          id,
		Name:        name,
		Type:        cmd.Type,
		FirstSeen:   timestamppb.New(appendedAt),
		LastSeen:    timestamppb.New(appendedAt),
		LastAddress: cmd.Address,
	}

	return &RegisterResult{
		ServerID:   id,
		ServerName: name,
	}, nil
}
