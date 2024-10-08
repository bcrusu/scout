package utils

import (
	"os"

	"github.com/bcrusu/graph/internal/errors"
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
