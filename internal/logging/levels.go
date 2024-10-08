package logging

import (
	"log/slog"
	"strings"

	"github.com/bcrusu/scout/internal/errors"
)

const (
	LevelTrace = Level(-8)
	LevelDebug = Level(slog.LevelDebug)
	LevelInfo  = Level(slog.LevelInfo)
	LevelWarn  = Level(slog.LevelWarn)
	LevelError = Level(slog.LevelError)
	LevelOff   = Level(100)
)

type Level = slog.Level

var levelNames = map[slog.Level]slog.Value{
	LevelTrace: slog.StringValue("TRACE"),
	LevelDebug: slog.StringValue("DEBUG"),
	LevelInfo:  slog.StringValue("INFO"),
	LevelWarn:  slog.StringValue("WARN"),
	LevelError: slog.StringValue("ERROR"),
}

var levelNamesInv = map[string]slog.Level{
	"TRACE": LevelTrace,
	"DEBUG": LevelDebug,
	"INFO":  LevelInfo,
	"WARN":  LevelWarn,
	"ERROR": LevelError,
}

func ParseLevel(str string) (Level, error) {
	upper := strings.ToUpper(str)
	result, ok := levelNamesInv[upper]
	if !ok {
		return 0, errors.Errorf("invalid log level %q", str)
	}

	return result, nil
}
