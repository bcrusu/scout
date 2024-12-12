package main

import (
	"os"

	"github.com/bcrusu/scout/internal/cmd"
	"github.com/bcrusu/scout/internal/control/server"
	"github.com/bcrusu/scout/internal/control/server/config"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "control",
		Short:         "Control plane server.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			cfg, err := getConfig(c)
			if err != nil {
				return err
			} else if err := config.Set(cfg); err != nil {
				return err
			}

			log := logging.New("control")
			s := server.NewServer()
			return utils.LifecycleRun(c.Context(), log, s)
		},
	}

	cmd.AddCommonParameters(c)

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

	if flags.AddressRPC != "" {
		cfg.RPC.Address = flags.AddressRPC
	}

	if flags.AddressHTTP != "" {
		cfg.HTTP.Address = flags.AddressHTTP
	}

	if flags.DataDir != "" {
		cfg.DataDir = flags.DataDir
	}

	if cfg.Register != nil && len(flags.Tags) > 0 {
		cfg.Register.Tags = flags.Tags
	}

	return cfg, nil
}
