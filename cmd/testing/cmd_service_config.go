package main

import (
	aconfig "github.com/bcrusu/scout/internal/api/server/config"
	cconfig "github.com/bcrusu/scout/internal/control/server/config"
	dconfig "github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/testing/services"
	"github.com/spf13/cobra"
)

func newServiceConfigCmd() *cobra.Command {
	getConfig := func(c *cobra.Command) (services.Config, error) {
		socketPath, err := getSocketPath(c)
		if err != nil {
			return services.Config{}, err
		}

		cConfig, err1 := loadConfigYaml[cconfig.Config](c, "config-control")
		dConfig, err2 := loadConfigYaml[dconfig.Config](c, "config-data")
		aConfig, err3 := loadConfigYaml[aconfig.Config](c, "config-api")

		if err := errors.Join(err1, err2, err3); err != nil {
			return services.Config{}, err
		}

		return services.Config{
			SocketPath:    socketPath,
			ClusterName:   clusterName,
			ControlNodes:  errors.Assert2(c.Flags().GetInt("nodes-control")),
			ControlConfig: cConfig,
			DataNodes:     errors.Assert2(c.Flags().GetInt("nodes-data")),
			DataConfig:    dConfig,
			APINodes:      errors.Assert2(c.Flags().GetInt("nodes-api")),
			APIConfig:     aConfig,
		}, nil
	}

	c := &cobra.Command{
		Use:     "config",
		Aliases: []string{"cfg"},
		Short:   "Configures services.",
		RunE: func(c *cobra.Command, args []string) error {
			config, err := getConfig(c)
			if err != nil {
				return err
			}

			return services.Configure(c.Context(), config)
		},
	}

	c.PersistentFlags().String("config-control", "configs/config-control.yaml", "Control server config template.")
	c.PersistentFlags().String("config-data", "configs/config-data.yaml", "Data server config template.")
	c.PersistentFlags().String("config-api", "configs/config-api.yaml", "API server config template.")
	c.PersistentFlags().Int("nodes-control", 1, "Control server node count.")
	c.PersistentFlags().Int("nodes-data", 0, "Data server node count. A value of 0 specifies all the remaining nodes.")
	c.PersistentFlags().Int("nodes-api", 1, "API server node count.")

	return c
}
