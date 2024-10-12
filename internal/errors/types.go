package errors

var (
	// InvalidRequest is a generic error that signals an invalid request.
	// For more specific errors should use ValidationError instead.
	InvalidRequest = Error("invalid request")

	// PermissionDenied lets the caller know that some things are not allowed.
	PermissionDenied = Error("permission denied")

	// Unavailable signals that the resource is not currently available.
	Unavailable = Error("resource unavailable")

	// NotFound signals that the item was not found.
	NotFound = Error("not found")

	// AlreadyExists signals that the item already exists.
	AlreadyExists = Error("already exists")

	// NotLeader signals that the invoked instance is not the group leader.
	NotLeader = Error("not leader")

	// NotRegistered signals that the accessed resource requires registration.
	NotRegistered = Error("not registered")

	// FailedPrecondition signals that the operation was rejected because the resource
	// state does not match the expected state.
	FailedPrecondition = Error("precondition failed")

	// ResourceExhausted used for quota and rate limiting.
	ResourceExhausted = Error("resource exhausted")

	// TransactionAborted signals that transaction was aborted. Clients should retry.
	TransactionAborted = Error("transaction was aborted")

	// CorruptedData indicates that the stored data is unreadable/corrupted.
	CorruptedData = Error("corrupted data")

	// TimeOffsetOutOfRange signals that the time offset between two servers is out of the allowed range.
	TimeOffsetOutOfRange = Error("time offset out of range")
)

// ValidationError is a validation error that carries extra information to callers.
type ValidationError struct {
	Message string
}

func (e ValidationError) Error() string {
	return e.Message
}
