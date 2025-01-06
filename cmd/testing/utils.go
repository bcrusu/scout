package main

import (
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	clusterName = "testing"
)

func newNodesClient(c *cobra.Command) (*nodes.Client, error) {
	socketPath, err := getSocketPath(c)
	if err != nil {
		return nil, err
	}
	return nodes.NewClient(socketPath)
}

func getWorkDir(c *cobra.Command) (string, error) {
	workDir, err := filepath.Abs(errors.Assert2(c.Flags().GetString("work-dir")))
	if err != nil {
		return "", errors.Wrap(err, "failed to resolve work dir")
	}

	return workDir, nil
}

func getSocketPath(c *cobra.Command) (string, error) {
	workDir, err := getWorkDir(c)
	if err != nil {
		return "", err
	}

	return joinSocketPath(workDir), nil
}

func joinSocketPath(workDir string) string {
	return path.Join(workDir, "nodes.socket")
}

func loadConfigYaml[T any](c *cobra.Command, flagName string) (T, error) {
	var cfg T
	filePath := errors.Assert2(c.Flags().GetString(flagName))

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return cfg, errors.Wrapf(err, "failed to read config file %s", filePath)
	}

	if err := utils.SetDefaults(&cfg); err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		return cfg, errors.Wrapf(err, "failed to unmarshal config file %s", filePath)
	}

	return cfg, nil
}

func checkRoot() error {
	uid := syscall.Getuid()
	if uid != 0 {
		return errors.Error("not running as root")
	}
	return nil
}
