package utils

import (
	"os"
	"strconv"
	"strings"

	"github.com/bcrusu/scout/internal/errors"
)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func MkdirsAll(paths ...string) error {
	for _, p := range paths {
		if err := MkdirAll(p); err != nil {
			return errors.Wrapf(err, "failed to create directory %q", p)
		}
	}
	return nil
}

func MkdirAll(path string) error {
	if exists, err := PathExists(path); err != nil {
		return err
	} else if !exists {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

// EnsureFile creates an empty file if it does not already exist.
func EnsureFile(filePath string) error {
	if file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); err != nil {
		return errors.Wrapf(err, "failed to open file %s", filePath)
	} else if err := file.Close(); err != nil {
		return errors.Wrapf(err, "faild to close file %s", filePath)
	}
	return nil
}

// EnsureEnvPath adds the target path to the PATH env variable.
func EnsureEnvPath(targetPath string) error {
	path := os.Getenv("PATH")
	set := MakeSet(strings.Split(path, ":"))

	if set[targetPath] {
		return nil
	}

	var newPath string
	if path == "" {
		newPath = targetPath
	} else {
		newPath = path + ":" + targetPath
	}

	if err := os.Setenv("PATH", newPath); err != nil {
		return errors.Wrap(err, "failed to set PATH")
	}

	return nil
}

// GetLastSuffix finds the last/max suffix in the specified dir.
func GetLastSuffix(dirPath, prefix string) (int, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, errors.Wrapf(err, "failed to read dir %s", dirPath)
	}

	result := 0

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		suffix := name[len(prefix):]
		val, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil {
			continue
		}

		result = max(result, int(val))
	}

	return result, nil
}
