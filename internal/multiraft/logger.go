package multiraft

import (
	"io"
	stdlog "log"

	"github.com/bcrusu/graph/internal/logging"
	"github.com/hashicorp/go-hclog"
)

var (
	_     hclog.Logger = (*logAdapter)(nil)
	hcLog              = newLogAdapter()
)

type logAdapter struct {
	log logging.LoggerNoContext
}

func newLogAdapter() *logAdapter {
	return &logAdapter{log: logging.WithComponent("hashicorp_raft").NoContext()}
}

func (l *logAdapter) Log(level hclog.Level, msg string, args ...any) {
	l.log.Log(getLogLevel(level), msg, args...)
}

func (l *logAdapter) Trace(msg string, args ...any) {
	l.log.Trace(msg, args...)
}

func (l *logAdapter) Debug(msg string, args ...any) {
	l.log.Debug(msg, args...)
}

func (l *logAdapter) Info(msg string, args ...any) {
	l.log.Info(msg, args...)
}

func (l *logAdapter) Warn(msg string, args ...any) {
	l.log.Warn(msg, args...)
}

func (l *logAdapter) Error(msg string, args ...any) {
	l.log.Error(msg, args...)
}

func (l *logAdapter) IsTrace() bool {
	return l.log.Enabled(logging.LevelTrace)
}

func (l *logAdapter) IsDebug() bool {
	return l.log.Enabled(logging.LevelDebug)
}

func (l *logAdapter) IsInfo() bool {
	return l.log.Enabled(logging.LevelInfo)
}

func (l *logAdapter) IsWarn() bool {
	return l.log.Enabled(logging.LevelWarn)
}

func (l *logAdapter) IsError() bool {
	return l.log.Enabled(logging.LevelError)
}

func (l *logAdapter) ImpliedArgs() []any {
	l.log.Warn("Unexpected call to ImpliedArgs")
	return nil
}

func (l *logAdapter) With(args ...any) hclog.Logger {
	return &logAdapter{l.log.With(args...)}
}

func (l *logAdapter) Name() string {
	return "hashicorp_raft"
}

func (l *logAdapter) Named(name string) hclog.Logger {
	l.log.Warn("Unexpected call to Named")
	return l
}

func (l *logAdapter) ResetNamed(name string) hclog.Logger {
	l.log.Warn("Unexpected call to ResetNamed")
	return l
}

func (l *logAdapter) SetLevel(level hclog.Level) {}

func (l *logAdapter) GetLevel() hclog.Level {
	switch logging.GetLevel() {
	case logging.LevelTrace:
		return hclog.Trace
	case logging.LevelDebug:
		return hclog.Debug
	case logging.LevelInfo:
		return hclog.Info
	case logging.LevelWarn:
		return hclog.Warn
	case logging.LevelError:
		return hclog.Error
	default:
		return hclog.Off
	}
}

func (l *logAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *stdlog.Logger {
	l.log.Warn("Unexpected call to StandardWriter")
	return stdlog.Default()
}

func (l *logAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	l.log.Warn("Unexpected call to StandardWriter")
	return stdlog.Default().Writer()
}

func getLogLevel(level hclog.Level) logging.Level {
	switch level {
	case hclog.Trace:
		return logging.LevelTrace
	case hclog.Debug:
		return logging.LevelDebug
	case hclog.Info:
		return logging.LevelInfo
	case hclog.Warn:
		return logging.LevelWarn
	case hclog.Error:
		return logging.LevelError
	case hclog.Off:
		return logging.LevelOff
	default:
		return logging.GetLevel()
	}
}
