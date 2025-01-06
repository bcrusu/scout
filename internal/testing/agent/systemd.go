package agent

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/bcrusu/scout/internal/errors"
)

func startService(serviceType ServiceType) error {
	return runSystemctlIfActive(serviceType, "start", false)
}

func stopService(serviceType ServiceType) error {
	return runSystemctlIfActive(serviceType, "stop", true)
}

func restartService(serviceType ServiceType) error {
	return runSystemctlIfActive(serviceType, "restart", true)
}

func freezeService(serviceType ServiceType) error {
	return runSystemctlIfActive(serviceType, "freeze", true)
}

func thawService(serviceType ServiceType) error {
	return runSystemctlIfActive(serviceType, "thaw", true)
}

func killService(serviceType ServiceType, signal string) error {
	signalParam := fmt.Sprintf("--signal=%s", signal)
	return runSystemctlIfActive(serviceType, "kill", true, signalParam)
}

func isServiceActive(serviceType ServiceType) (bool, error) {
	serviceName := getServiceName(serviceType)
	out, err := runSystemctl("show", "-P", "ActiveState", serviceName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to determine %s active state", serviceName)
	}

	active := strings.TrimSpace(string(out)) == "active"
	return active, nil
}

func runSystemctl(args ...string) ([]byte, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := exec.Command("systemctl", args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err != nil {
		return nil, errors.Wrapf(err, "systemctl %v failed", args)
	}

	return stdout.Bytes(), nil
}

func runSystemctlIfActive(serviceType ServiceType, cmd string, ifActive bool, cmdArgs ...string) error {
	serviceName := getServiceName(serviceType)
	if active, err := isServiceActive(serviceType); err != nil {
		return err
	} else if active != ifActive {
		return nil
	}

	allArgs := []string{cmd, serviceName}
	allArgs = append(allArgs, cmdArgs...)

	_, err := runSystemctl(allArgs...)
	if err != nil {
		return errors.Wrapf(err, "failed to %s %s", cmd, serviceName)
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
