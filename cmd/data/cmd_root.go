package main

import (
	"os"

	"github.com/bcrusu/graph/internal/cmd"
	"github.com/bcrusu/graph/internal/data/server/config"
	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newRootCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "data",
		Short:         "Graph data storage server.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			cmd.SetLogLevel(c)
			cfg, err := getConfig(c)
			if err != nil {
				return err
			}
			return config.Set(cfg)
		},
	}

	cmd.AddCommonParameters(c)

	c.AddCommand(
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

	return cfg, nil
}
