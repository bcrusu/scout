package logging

import (
	"context"
	"fmt"
	"log/slog"
)

// Logger provides logging functionality.
type Logger interface {
	Log(ctx context.Context, level Level, msg string, args ...any)
	Trace(ctx context.Context, msg string, args ...any)
	Tracef(ctx context.Context, format string, args ...any)
	Debug(ctx context.Context, msg string, args ...any)
	Debugf(ctx context.Context, format string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Infof(ctx context.Context, format string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Warnf(ctx context.Context, format string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
	Errorf(ctx context.Context, format string, args ...any)
	With(args ...any) Logger
	WithError(err error) Logger
	WithComponent(name string) Logger
	Enabled(Level) bool
	NoContext() LoggerNoContext
}

type LoggerNoContext interface {
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
	With(args ...any) LoggerNoContext
	WithError(err error) LoggerNoContext
	WithComponent(name string) LoggerNoContext
	WithContext() Logger
	Enabled(Level) bool
}

type slogLogger struct {
	slog *slog.Logger
}

func (l *slogLogger) Log(ctx context.Context, level Level, msg string, args ...any) {
	l.slog.Log(ctx, level, msg, args...)
}

func (l *slogLogger) Trace(ctx context.Context, msg string, args ...any) {
	l.Log(ctx, LevelTrace, msg, args...)
}

func (l *slogLogger) Tracef(ctx context.Context, format string, args ...any) {
	if l.Enabled(LevelTrace) {
		l.Trace(ctx, fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Debug(ctx context.Context, msg string, args ...any) {
	l.Log(ctx, LevelDebug, msg, args...)
}

func (l *slogLogger) Debugf(ctx context.Context, format string, args ...any) {
	if l.Enabled(LevelDebug) {
		l.Debug(ctx, fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Info(ctx context.Context, msg string, args ...any) {
	l.Log(ctx, LevelInfo, msg, args...)
}

func (l *slogLogger) Infof(ctx context.Context, format string, args ...any) {
	if l.Enabled(LevelInfo) {
		l.Info(ctx, fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Warn(ctx context.Context, msg string, args ...any) {
	l.Log(ctx, LevelWarn, msg, args...)
}

func (l *slogLogger) Warnf(ctx context.Context, format string, args ...any) {
	if l.Enabled(LevelWarn) {
		l.Warn(ctx, fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) Error(ctx context.Context, msg string, args ...any) {
	l.Log(ctx, LevelError, msg, args...)
}

func (l *slogLogger) Errorf(ctx context.Context, format string, args ...any) {
	if l.Enabled(LevelError) {
		l.Error(ctx, fmt.Sprintf(format, args...))
	}
}

func (l *slogLogger) With(args ...any) Logger {
	return &slogLogger{l.slog.With(args...)}
}

func (l *slogLogger) WithError(err error) Logger {
	if err == nil {
		return l
	}
	return l.With("error", err)
}

func (l *slogLogger) WithComponent(name string) Logger {
	return l.With("comp", name)
}

func (l *slogLogger) Enabled(level Level) bool {
	return l.slog.Handler().Enabled(context.Background(), level)
}

func (l *slogLogger) NoContext() LoggerNoContext {
	return &loggerNoCtx{l}
}

type loggerNoCtx struct {
	log Logger
}

func (l *loggerNoCtx) Log(level Level, msg string, args ...any) {
	l.log.Log(context.Background(), level, msg, args...)
}

func (l *loggerNoCtx) Trace(msg string, args ...any) {
	l.log.Trace(context.Background(), msg, args...)
}

func (l *loggerNoCtx) Tracef(format string, args ...any) {
	l.log.Tracef(context.Background(), format, args...)
}

func (l *loggerNoCtx) Debug(msg string, args ...any) {
	l.log.Debug(context.Background(), msg, args...)
}

func (l *loggerNoCtx) Debugf(format string, args ...any) {
	l.log.Debugf(context.Background(), format, args...)
}

func (l *loggerNoCtx) Info(msg string, args ...any) {
	l.log.Info(context.Background(), msg, args...)
}

func (l *loggerNoCtx) Infof(format string, args ...any) {
	l.log.Infof(context.Background(), format, args...)
}

func (l *loggerNoCtx) Warn(msg string, args ...any) {
	l.log.Warn(context.Background(), msg, args...)
}

func (l *loggerNoCtx) Warnf(format string, args ...any) {
	l.log.Warnf(context.Background(), format, args...)
}

func (l *loggerNoCtx) Error(msg string, args ...any) {
	l.log.Error(context.Background(), msg, args...)
}

func (l *loggerNoCtx) Errorf(format string, args ...any) {
	l.log.Errorf(context.Background(), format, args...)
}

func (l *loggerNoCtx) With(args ...any) LoggerNoContext {
	return &loggerNoCtx{l.log.With(args...)}
}

func (l *loggerNoCtx) WithError(err error) LoggerNoContext {
	return &loggerNoCtx{l.log.WithError(err)}
}

func (l *loggerNoCtx) WithComponent(name string) LoggerNoContext {
	return &loggerNoCtx{l.log.WithComponent(name)}
}

func (l *loggerNoCtx) Enabled(level Level) bool {
	return l.log.Enabled(level)
}

func (l *loggerNoCtx) WithContext() Logger {
	return l.log
}
