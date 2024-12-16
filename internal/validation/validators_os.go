package validation

import (
	"fmt"
	"os"
	"reflect"
	"strings"
)

func pathExists(val reflect.Value, param string) string {
	path, errMsg := getString(val)
	if errMsg != "" {
		return errMsg
	}

	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Sprintf("path %s does not exist", param)
		}
		return fmt.Sprintf("could not determine path %s status", path)
	}

	if param == "" {
		return ""
	}

	mode := stat.Mode()

	switch strings.ToLower(param) {
	case "dir":
		if !stat.IsDir() {
			return fmt.Sprintf("path %s is not a directory", path)
		}
	case "file":
		if mode&os.ModeType != 0 {
			return fmt.Sprintf("path %s is not a file", path)
		}
	case "socket":
		if mode&os.ModeSocket == 0 {
			return fmt.Sprintf("path %s is not a socket", path)
		}
	default:
		return "invalid param value"
	}

	return ""
}

func pathNotExists(val reflect.Value, param string) string {
	path, errMsg := getString(val)
	if errMsg != "" {
		return errMsg
	}

	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		return fmt.Sprintf("could not determine path %s status", path)
	}

	return fmt.Sprintf("path %s exists", path)
}
