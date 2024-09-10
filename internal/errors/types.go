package errors

var (
	// InvalidRequest is a generic error that signals an invalid request.
	// For more specific errors should use ValidationError instead.
	InvalidRequest = Error("invalid request")

	// Unavailable signals that the resource is not currently available.
	Unavailable = Error("resource unavailable")

	// NotFound signals that the item was not found.
	NotFound = Error("not found")

	// NotLeader signals that the invoked instance is not the group leader.
	NotLeader = Error("not leader")

	// UnknownLeader is a transient error which signals that the group leader is not known.
	UnknownLeader = Error("unknown leader")

	// NotRegistered signals that the accessed resource requires registration.
	NotRegistered = Error("not registered")
)

// ValidationError is a validation error that carries extra information to callers.
type ValidationError struct {
	Message string
}

func (e ValidationError) Error() string {
	return e.Message
}
