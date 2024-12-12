package main

import (
	"os"
	"path"
	"path/filepath"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
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

	socketPath := joinSocketPath(workDir)
	if err := pathMustExist(false, socketPath); err != nil {
		return "", err
	}

	return socketPath, nil
}

func joinSocketPath(workDir string) string {
	return path.Join(workDir, "nodes.socket")
}

func pathMustExist(isDir bool, paths ...string) error {
	for _, path := range paths {
		stat, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return errors.Wrapf(err, "path %s does not exist", path)
			}
			return errors.Wrapf(err, "could not determine path %s status", path)
		}

		if isDir && !stat.IsDir() {
			return errors.Wrapf(err, "path %s is not a dir", path)
		}
		if !isDir && stat.IsDir() {
			return errors.Wrapf(err, "path %s is not a file", path)
		}
	}

	return nil
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
