package session

import "github.com/bcrusu/scout/internal/eventbus"

type refreshDataServers struct{}

// RefreshDataServers notifies the current session to retreive the
// latest version of DataServers from the control plane.
func RefreshDataServers() {
	eventbus.TryPublish(refreshDataServers{})
}
