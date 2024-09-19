package main

import (
	"os"

	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/control/server/config"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newRootCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "control",
		Short:         "Graph control plane server.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRun: func(c *cobra.Command, args []string) {
			cmd.SetLogLevel(c)
		},
	}

	cmd.AddCommonParameters(c)

	c.AddCommand(
		newBootstrapCmd(),
		newJoinCmd(),
		newStartCmd(),
	)

	return c
}

func getConfig(c *cobra.Command) (config.Config, error) {
	flags, err := cmd.GetConfigFlags(c)
	if err != nil {
		return config.Config{}, err
	}

	data, err := os.ReadFile(flags.ConfigFile)
	if err != nil {
		return config.Config{}, errors.Error("failed to read config file")
	}

	var cfg config.Config
	if err := utils.SetDefaults(&cfg); err != nil {
		return config.Config{}, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return config.Config{}, errors.Error("failed to parse config file")
	}

	if flags.BindAddress != "" {
		cfg.Server.BindAddress = flags.BindAddress
	}

	if flags.DataDir != "" {
		cfg.DataDir = flags.DataDir
	}

	if err := validateConfig(cfg); err != nil {
		return config.Config{}, err
	}

	return cfg, nil
}

func validateConfig(cfg config.Config) error {
	// TODO: validate cfg

	return nil
}
