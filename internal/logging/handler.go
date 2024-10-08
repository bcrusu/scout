package logging

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/bcrusu/scout/internal/tracing"
)

type handler struct {
	*slog.TextHandler
}

func newHandler() *handler {
	return &handler{
		TextHandler: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource:   false,
			Level:       dynamicLevel,
			ReplaceAttr: replaceAttr,
		}),
	}
}

func (h *handler) Handle(ctx context.Context, record slog.Record) error {
	if traceID, ok := tracing.GetTraceID(ctx); ok {
		record.Add("trace", traceID)
	}

	return h.TextHandler.Handle(ctx, record)
}

func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	switch a.Key {
	case slog.TimeKey:
		if timestampFormat != "" {
			t, ok := a.Value.Any().(time.Time)
			if ok {
				a.Value = slog.StringValue(t.Format(timestampFormat))
			}
		}
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
