package cmd

import (
	"github.com/bcrusu/scout/internal/errors"
	"github.com/spf13/cobra"
)

type ConfigFlags struct {
	ConfigFile  string
	BindAddress string
	DataDir     string
}

func AddCommonParameters(c *cobra.Command) {
	c.PersistentFlags().String("config-file", "config.yaml", "Specifies the configuration file path.")
	c.PersistentFlags().String("bind-address", "", "The address to serve on.")
	c.PersistentFlags().String("data-dir", "", "Directory to store data.")
}

func GetConfigFlags(c *cobra.Command) (ConfigFlags, error) {
	bindAddress, err1 := c.Flags().GetString("bind-address")
	dataDir, err2 := c.Flags().GetString("data-dir")
	configFile, err3 := c.Flags().GetString("config-file")
	if err3 == nil && configFile == "" {
		err3 = errors.Error("missing config-file flag")
	}

	err := errors.Join(err1, err2, err3)
	if err != nil {
		return ConfigFlags{}, err
	}

	return ConfigFlags{
		ConfigFile:  configFile,
		BindAddress: bindAddress,
		DataDir:     dataDir,
	}, nil
}
