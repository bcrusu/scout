package tracing

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	traceIDPrefix     = getRandTraceIDPrefix()
	traceIDCounter    = &atomic.Uint64{}
	traceIDServerName = &atomic.Pointer[string]{}
)

type ctxKeyTraceID struct{}

// Returns a new context
func NewContext() context.Context {
	return WithNewTraceID(context.Background())
}

// WithTraceID returns a copy of the context populated with the provided trace identifier.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		panic("trace identifier is empty")
	}

	return context.WithValue(ctx, ctxKeyTraceID{}, traceID)
}

// WithTraceID returns a copy of the context populated with a new trace value.
func WithNewTraceID(ctx context.Context) context.Context {
	traceID := getNextTraceID()
	return context.WithValue(ctx, ctxKeyTraceID{}, traceID)
}

// GetTraceID extracts the trace identifier from context
func GetTraceID(ctx context.Context) string {
	v := ctx.Value(ctxKeyTraceID{})
	if v == nil {
		return ""
	}

	return v.(string)
}

// SetServerName sets, only once, the current server name.
func SetServerName(serverName string) {
	if v := traceIDServerName.Load(); v != nil {
		panic("server name is already set.")
	}

	traceIDServerName.Store(&serverName)
}

func getNextTraceID() string {
	id := traceIDCounter.Add(1)

	if v := traceIDServerName.Load(); v != nil {
		return fmt.Sprintf("%s_%d_%s", traceIDPrefix, id, *v)
	} else {
		return fmt.Sprintf("%s_%d", traceIDPrefix, id)
	}
}

func getRandTraceIDPrefix() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	bytes := make([]byte, 4)
	r.Read(bytes)

	str := fmt.Sprintf("%X", bytes)
	return str
}
