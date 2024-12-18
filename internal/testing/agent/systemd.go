package agent

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/bcrusu/scout/internal/errors"
)

func startService(serviceType ServiceType) error {
	if active, err := isServiceActive(serviceType); err != nil {
		return err
	} else if active {
		return nil
	}

	_, err := runSystemctl("start", getServiceName(serviceType))
	if err != nil {
		return errors.Wrap(err, "failed to start service")
	}
	return nil
}

func stopService(serviceType ServiceType) error {
	if active, err := isServiceActive(serviceType); err != nil {
		return err
	} else if !active {
		return nil
	}

	_, err := runSystemctl("stop", getServiceName(serviceType))
	if err != nil {
		return errors.Wrap(err, "failed to stop service")
	}
	return nil
}

func stopAllServices() error {
	all := []ServiceType{
		ServiceType_Control,
		ServiceType_Data,
		ServiceType_Api,
	}

	for _, x := range all {
		if err := stopService(x); err != nil {
			return errors.Wrapf(err, "failed to stop %s service", x)
		}
	}

	return nil
}

func restartService(serviceType ServiceType) error {
	if active, err := isServiceActive(serviceType); err != nil {
		return err
	} else if !active {
		return nil
	}

	_, err := runSystemctl("restart", getServiceName(serviceType))
	if err != nil {
		return errors.Wrap(err, "failed to restart service")
	}
	return nil
}

func isServiceActive(serviceType ServiceType) (bool, error) {
	out, err := runSystemctl("show", "-P", "ActiveState", getServiceName(serviceType))
	if err != nil {
		return false, errors.Wrap(err, "failed to determine service state")
	}

	active := strings.TrimSpace(string(out)) == "active"
	return active, nil
}

func getServiceName(serviceType ServiceType) string {
	switch serviceType {
	case ServiceType_Control:
		return "scout@control.service"
	case ServiceType_Data:
		return "scout@data.service"
	case ServiceType_Api:
		return "scout@api.service"
	default:
		panic(fmt.Sprintf("Unknown service type %s", serviceType))
	}
}

func runSystemctl(args ...string) ([]byte, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := exec.Command("systemctl", args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err != nil {
		log.WithError(err).Error("systemctl cmd failed", "args", args, "stdout", stdout.String(), "stderr", stderr.String())
		return nil, err
	}

	return stdout.Bytes(), nil
}
