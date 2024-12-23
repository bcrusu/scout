package logging

import (
	"context"
	"log/slog"
	"os"

	"github.com/bcrusu/scout/internal/tracing"
)

type handler struct {
	name  string
	inner slog.Handler
}

func newHandler(name string, level slog.Leveler) *handler {
	return &handler{
		name: name,
		inner: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource:   false,
			Level:       level,
			ReplaceAttr: replaceAttr,
		}),
	}
}

func (h *handler) Enabled(ctx context.Context, level Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *handler) Handle(ctx context.Context, record slog.Record) error {
	record.Add("com", h.name)

	if traceID := tracing.GetTraceID(ctx); traceID != "" {
		record.Add("trace", traceID)
	}

	return h.inner.Handle(ctx, record)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &handler{h.name, h.inner.WithAttrs(attrs)}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{h.name, h.inner.WithGroup(name)}
}

func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	switch a.Key {
	case slog.LevelKey:
		level := a.Value.Any().(slog.Level)

		name, ok := levelNames[level]
		if ok {
			a.Value = name
		} else {
			a.Value = slog.StringValue(level.String())
		}
	}

	return a
}
