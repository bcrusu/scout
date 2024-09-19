package main

import (
	"os"

	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/data/server"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/bcrusu/graph/internal/validation"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newRootCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "data",
		Short:         "Graph data storage server.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRun: func(c *cobra.Command, args []string) {
			cmd.SetLogLevel(c)
		},
	}

	cmd.AddCommonParameters(c)

	c.AddCommand(
		newJoinCmd(),
		newStartCmd(),
	)

	return c
}

func getConfig(c *cobra.Command) (server.Config, error) {
	flags, err := cmd.GetConfigFlags(c)
	if err != nil {
		return server.Config{}, err
	}

	data, err := os.ReadFile(flags.ConfigFile)
	if err != nil {
		return server.Config{}, errors.Error("failed to read config file")
	}

	var cfg server.Config
	if err := utils.SetDefaults(&cfg); err != nil {
		return server.Config{}, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return server.Config{}, errors.Error("failed to parse config file")
	}

	if flags.BindAddress != "" {
		cfg.Server.BindAddress = flags.BindAddress
	}

	if flags.DataDir != "" {
		cfg.DataDir = flags.DataDir
	}

	if err := validation.Validate(cfg); err != nil {
		return server.Config{}, err
	}

	return cfg, nil
}
