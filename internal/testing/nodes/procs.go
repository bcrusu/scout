package nodes

import (
	"strings"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/shirou/gopsutil/v4/process"
)

func loadProcs(firecrackerPath string) (map[string]int, error) {
	pids, err := process.Pids()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list procs")
	}

	result := map[string]int{}

	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			if unknownProc(err) {
				continue
			}
			return nil, errors.Wrapf(err, "NewProcess call failed for pid=%d", pid)
		}

		exePath, err := proc.Exe()
		if err != nil {
			if unknownProc(err) {
				continue
			}
			return nil, errors.Wrapf(err, "Pocess.Exe call failed for pid=%d", pid)
		} else if exePath != firecrackerPath {
			continue
		}

		cmdLine, err := proc.Cmdline()
		if err != nil {
			if unknownProc(err) {
				continue
			}
			return nil, errors.Wrapf(err, "Pocess.Cmdline call failed for pid=%d", pid)
		}

		id := extractId(cmdLine)
		if !strings.HasPrefix(id, nodeIdPrefix) {
			continue
		}

		result[id] = int(pid)
	}

	return result, nil
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

func unknownProc(err error) bool {
	str := err.Error()
	return strings.Contains(str, "no such file or directory")
}
