package cmd

import (
	"github.com/bcrusu/scout/internal/errors"
	"github.com/spf13/cobra"
)

type ConfigFlags struct {
	ConfigFile  string
	AddressRPC  string
	AddressHTTP string
	DataDir     string
	Tags        []string
}

// AddCommonParameters allows certain overrides for yaml config options.
func AddCommonParameters(c *cobra.Command) {
	c.PersistentFlags().String("config-file", "config.yaml", "Specifies the configuration file path.")
	c.PersistentFlags().String("address-rpc", "", "The address to serve on the RPC server.")
	c.PersistentFlags().String("address-http", "", "The address to serve on the HTTP server.")
	c.PersistentFlags().String("data-dir", "", "Directory to store data.")
	c.PersistentFlags().StringSlice("tags", nil, "Server tags.")
}

func GetConfigFlags(c *cobra.Command) (ConfigFlags, error) {
	addressRpc, err1 := c.Flags().GetString("address-rpc")
	dataDir, err2 := c.Flags().GetString("data-dir")
	configFile, err3 := c.Flags().GetString("config-file")
	if err3 == nil && configFile == "" {
		err3 = errors.Error("missing config-file flag")
	}
	tags, err4 := c.Flags().GetStringSlice("tags")
	addressHttp, err5 := c.Flags().GetString("address-http")

	err := errors.Join(err1, err2, err3, err4, err5)
	if err != nil {
		return ConfigFlags{}, err
	}

	return ConfigFlags{
		ConfigFile:  configFile,
		AddressRPC:  addressRpc,
		AddressHTTP: addressHttp,
		DataDir:     dataDir,
		Tags:        tags,
	}, nil
}
