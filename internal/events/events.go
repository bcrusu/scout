package events

// This file contains reusable message types relevant for all services.

// RefreshDataServers requests that the latest version of DataServers be
// retreived from the control plane.
type RefreshDataServers struct{}

// TryPublishRefreshDataServers publishes the RefreshDataServers message.
func TryPublishRefreshDataServers() {
	TryPublish(RefreshDataServers{})
}
