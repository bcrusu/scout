package tracing

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	ctxKeyTraceID contextKey = iota
)

var (
	traceIDPrefix  = getTraceIDPrefix()
	traceIDCounter = &atomic.Uint64{}
)

type contextKey int

// Returns a new context
func NewContext() context.Context {
	return WithTraceID(context.Background(), "")
}

// WithTraceID returns a copy of the context populated with the provided trace identifier.
// If the provided identifier is empty, a new value will be generated
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		traceID = getNextTraceID()
	}

	return context.WithValue(ctx, ctxKeyTraceID, traceID)
}

// GetTraceID extracts the trace identifier from context
func GetTraceID(ctx context.Context) (string, bool) {
	v := ctx.Value(ctxKeyTraceID)
	if v == nil {
		return "", false
	}

	return v.(string), true
}

func getNextTraceID() string {
	id := traceIDCounter.Add(1)
	return fmt.Sprintf("%s_%d", traceIDPrefix, id) // TODO: use server name vs. random prefix
}

func getTraceIDPrefix() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	bytes := make([]byte, 4)
	r.Read(bytes)

	str := fmt.Sprintf("%X", bytes)
	return str
}
