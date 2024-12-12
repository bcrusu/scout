package agent

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/bcrusu/scout/internal/errors"
)

func startService(ctx context.Context, serviceType ServiceType) error {
	if active, err := isServiceActive(ctx, serviceType); err != nil {
		return err
	} else if active {
		return nil
	}

	_, err := runSystemctl(ctx, "start", getServiceName(serviceType))
	if err != nil {
		return errors.Wrap(err, "failed to start service")
	}
	return nil
}

func stopService(ctx context.Context, serviceType ServiceType) error {
	if active, err := isServiceActive(ctx, serviceType); err != nil {
		return err
	} else if !active {
		return nil
	}

	_, err := runSystemctl(ctx, "stop", getServiceName(serviceType))
	if err != nil {
		return errors.Wrap(err, "failed to stop service")
	}
	return nil
}

func isServiceActive(ctx context.Context, serviceType ServiceType) (bool, error) {
	out, err := runSystemctl(ctx, "show", "-P", "ActiveState", getServiceName(serviceType))
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
		panic(fmt.Sprintf("Unknown server type %s", serviceType))
	}
}

func runSystemctl(ctx context.Context, args ...string) ([]byte, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := exec.CommandContext(ctx, "systemctl", args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if err != nil {
		log.WithError(err).Error("systemctl cmd failed", "args", args, "stdout", stdout.String(), "stderr", stderr.String())
		return nil, err
	}

	return stdout.Bytes(), nil
}
