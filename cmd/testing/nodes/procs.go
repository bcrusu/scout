package nodes

import (
	"strings"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/shirou/gopsutil/v4/process"
)

var (
	pidCache = map[string]int32{} // map[node_ID]PID
)

func InitPIDCache(firecrackerPath string) error {
	pids, err := process.Pids()
	if err != nil {
		return errors.Wrap(err, "failed to list process pids.")
	}

	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			log.WithError(err).Log(getLogLevel(err), "Failed to create process.", "pid", pid)
			continue
		}

		exePath, err := proc.Exe()
		if err != nil {
			log.WithError(err).Log(getLogLevel(err), "Failed to get process exe.", "pid", pid)
			continue
		} else if exePath != firecrackerPath {
			continue
		}

		cmdLine, err := proc.Cmdline()
		if err != nil {
			log.WithError(err).Log(getLogLevel(err), "Failed to get cmd line.", "pid", pid)
			continue
		}

		id := extractId(cmdLine)
		if !strings.HasPrefix(id, nodeIdPrefix) {
			continue
		}

		pidCache[id] = pid
	}

	return nil
}

func extractId(cmdLine string) string {
	idx := strings.Index(cmdLine, "--id ")
	if idx == -1 {
		return ""
	}

	id := cmdLine[idx+5:]
	idx = strings.Index(id, " ")
	return id[:idx]
}

func getLogLevel(err error) logging.Level {
	str := err.Error()
	if strings.Contains(str, "no such file or directory") {
		return logging.LevelOff
	}

	return logging.LevelDebug
}
