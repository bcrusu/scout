package logging

import (
	"log/slog"
	"strings"
	"sync"

	"github.com/bcrusu/scout/internal/errors"
)

var (
	lock         sync.Mutex
	levels       = map[string]*slog.LevelVar{}
	defaultLevel = LevelInfo
)

// New returns a new named Logger.
func New(name string) Logger {
	lock.Lock()
	defer lock.Unlock()

	lvl, ok := levels[name]
	if !ok {
		lvl = new(slog.LevelVar)
		lvl.Set(defaultLevel)
		levels[name] = lvl
	}

	return newSlogLogger(name, lvl)
}

// SetLevels configures the log levels. The expected format is:
// 'name1:level1,name2:level2...' with the wildcard name '*'
// representing the default level.
func SetLevels(str string) error {
	newLevels := map[string]Level{}

	for _, level := range strings.Split(str, ",") {
		parts := strings.Split(level, ":")
		if len(parts) != 2 {
			return errors.Error("invalid levels format")
		}

		lvl, err := parseLevel(parts[1])
		if err != nil {
			return err
		}
		newLevels[parts[0]] = lvl
	}

	lock.Lock()
	defer lock.Unlock()

	if level, ok := newLevels["*"]; ok {
		defaultLevel = level

		for name, lvl := range levels {
			if _, ok := newLevels[name]; !ok {
				lvl.Set(defaultLevel)
			}
		}
	}

	for name, level := range newLevels {
		if lvl, ok := levels[name]; ok {
			lvl.Set(level)
		} else {
			lvl = new(slog.LevelVar)
			lvl.Set(level)
			levels[name] = lvl
		}
	}

	return nil
}
