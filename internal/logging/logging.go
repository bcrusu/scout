package logging

import (
	"strings"
	"sync"

	"github.com/bcrusu/scout/internal/errors"
)

const (
	timestampFormat = "15:04:05.000"
)

var (
	lock         sync.Mutex
	logs         = map[string]*slogLogger{}
	defaultLevel = LevelInfo
)

// New returns a new named Logger.
func New(name string) Logger {
	lock.Lock()

	log, ok := logs[name]
	if !ok {
		log = newSlogLogger(name, defaultLevel)
		logs[name] = log
	}

	lock.Unlock()
	return log
}

// SetLevels configures the log levels. The expected format is:
// 'name1:level1,name2:level2...' with the wildcard name '*'
// representing the default level.
func SetLevels(str string) error {
	levels := map[string]Level{}

	for _, level := range strings.Split(str, ",") {
		parts := strings.Split(level, ":")
		if len(parts) != 2 {
			return errors.Error("invalid levels format")
		}

		lvl, err := parseLevel(parts[1])
		if err != nil {
			return err
		}
		levels[parts[0]] = lvl
	}

	lock.Lock()

	if level, ok := levels["*"]; ok {
		defaultLevel = level

		for name, log := range logs {
			if _, ok := levels[name]; !ok {
				log.setLevel(defaultLevel)
			}
		}
	}

	for name, level := range levels {
		if log, ok := logs[name]; ok {
			log.setLevel(level)
		} else {
			log = newSlogLogger(name, level)
			logs[name] = log
		}
	}

	lock.Unlock()
	return nil
}
