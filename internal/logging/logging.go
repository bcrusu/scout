package logging

import (
	"context"
	"log/slog"
)

var (
	dynamicLevel    = getDefaultLevel()
	logger          = getDefaultLogger()
	timestampFormat = "15:04:05.000"
)

func Log(ctx context.Context, l Level, msg string, args ...any) { logger.Log(ctx, l, msg, args...) }
func Trace(ctx context.Context, msg string, args ...any)        { logger.Trace(ctx, msg, args...) }
func Tracef(ctx context.Context, fmt string, args ...any)       { logger.Tracef(ctx, fmt, args...) }
func Debug(ctx context.Context, msg string, args ...any)        { logger.Debug(ctx, msg, args...) }
func Debugf(ctx context.Context, fmt string, args ...any)       { logger.Debugf(ctx, fmt, args...) }
func Info(ctx context.Context, msg string, args ...any)         { logger.Info(ctx, msg, args...) }
func Infof(ctx context.Context, fmt string, args ...any)        { logger.Infof(ctx, fmt, args...) }
func Warn(ctx context.Context, msg string, args ...any)         { logger.Warn(ctx, msg, args...) }
func Warnf(ctx context.Context, fmt string, args ...any)        { logger.Warnf(ctx, fmt, args...) }
func Error(ctx context.Context, msg string, args ...any)        { logger.Error(ctx, msg, args...) }
func Errorf(ctx context.Context, fmt string, args ...any)       { logger.Errorf(ctx, fmt, args...) }
func With(args ...any) Logger                                   { return logger.With(args...) }
func WithError(err error) Logger                                { return logger.WithError(err) }
func WithComponent(name string) Logger                          { return logger.WithComponent(name) }
func Enabled(level Level) bool                                  { return logger.Enabled(level) }
func NoContext() LoggerNoContext                                { return logger.NoContext() }

// GetLog returns the logger
func GetLoger() Logger {
	return logger
}

// SetLevel sets the current logging level
func SetLevel(level Level) {
	dynamicLevel.Set(level)
}

// GetLevel returns current logging level
func GetLevel() Level {
	return dynamicLevel.Level()
}

// Disable turns off logging
func Disable() {
	SetLevel(LevelOff)
}

func getDefaultLevel() *slog.LevelVar {
	l := new(slog.LevelVar)
	l.Set(LevelInfo)
	return l
}

func getDefaultLogger() Logger {
	log := slog.New(newHandler())
	return &slogLogger{log}
}
