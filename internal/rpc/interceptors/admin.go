package interceptors

import "github.com/bcrusu/scout/internal/control"

var (
	adminEndpoints = map[string]bool{
		control.Service_GetClusterInfo_FullMethodName: true,
	}
)

// isAdmin allows disabling certain checks for admin cli tool calls.
// Currently it also disables the auth interceptor which is not ideal.
func isAdmin(methodName string) bool {
	return adminEndpoints[methodName]
}
