package logging

import (
	"context"
	"fmt"
	"log/slog"
)

// Logger provides logging functionality.
type Logger interface {
	Log(level Level, msg string, args ...any)
	Trace(msg string, args ...any)
	Tracef(format string, args ...any)
	Debug(msg string, args ...any)
	Debugf(format string, args ...any)
	Info(msg string, args ...any)
	Infof(format string, args ...any)
	Warn(msg string, args ...any)
	Warnf(format string, args ...any)
	Error(msg string, args ...any)
	Errorf(format string, args ...any)
	With(args ...any) Logger
	WithError(err error) Logger
	WithTrace(trace string) Logger
	WithContext(ctx context.Context) Logger
	Enabled(Level) bool
	GetLevel() Level
}

type slogLogger struct {
	level *slog.LevelVar
	slog  *slog.Logger
	ctx   context.Context
}

func newSlogLogger(name string, level *slog.LevelVar) *slogLogger {
	slog := slog.New(newHandler(name, level))

	return &slogLogger{
		level: level,
		slog:  slog,
		ctx:   context.Background(),
	}
}

func (l *slogLogger) Log(level Level, msg string, args ...any) {
	l.slog.Log(l.ctx, level, msg, args...)
}

func (l *slogLogger) Trace(msg string, args ...any) {
	l.Log(LevelTrace, msg, args...)
}

func (l *slogLogger) Tracef(format string, args ...any) {
	if l.Enabled(LevelTrace) {
		l.Trace(fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Debug(msg string, args ...any) {
	l.Log(LevelDebug, msg, args...)
}

func (l *slogLogger) Debugf(format string, args ...any) {
	if l.Enabled(LevelDebug) {
		l.Debug(fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Info(msg string, args ...any) {
	l.Log(LevelInfo, msg, args...)
}

func (l *slogLogger) Infof(format string, args ...any) {
	if l.Enabled(LevelInfo) {
		l.Info(fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Warn(msg string, args ...any) {
	l.Log(LevelWarn, msg, args...)
}

func (l *slogLogger) Warnf(format string, args ...any) {
	if l.Enabled(LevelWarn) {
		l.Warn(fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Error(msg string, args ...any) {
	l.Log(LevelError, msg, args...)
}

func (l *slogLogger) Errorf(format string, args ...any) {
	if l.Enabled(LevelError) {
		l.Error(fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) With(args ...any) Logger {
	return &slogLogger{l.level, l.slog.With(args...), l.ctx}
}

func (l *slogLogger) WithError(err error) Logger {
	if err == nil {
		return l
	}
	return l.With("error", err)
}

func (l *slogLogger) WithTrace(trace string) Logger {
	if trace == "" {
		return l
	}
	return l.With("trace", trace)
}

func (l *slogLogger) Enabled(level Level) bool {
	return l.slog.Enabled(context.Background(), level)
}

func (l *slogLogger) GetLevel() Level {
	return l.level.Level()
}

func (l *slogLogger) WithContext(ctx context.Context) Logger {
	return &slogLogger{l.level, l.slog, ctx}
}
