package cmd

import (
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/rpc"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

func AddAllParameters(c *cobra.Command) {
	AddCommonParameters(c)
	AddDiscoveryParameters(c)
	AddServerConfigParameters(c)
}

func AddCommonParameters(c *cobra.Command) {
	c.PersistentFlags().String("cluster-name", "", "Servers are only allowed to join this cluster.")
	c.PersistentFlags().String("data-dir", "", "Directory to store data.")
	c.PersistentFlags().String("log-level", "info", "Logging level.")
}

func AddDiscoveryParameters(c *cobra.Command) {
	c.PersistentFlags().String("discovery", "static", "Cluster discovery method. Possible values are 'static' or 'dns'.")
	c.PersistentFlags().StringSlice("peers", nil, "Server address list used for static discovery.")
	c.PersistentFlags().String("target", "", "The gRPC resolver target. For DNS discovery the expected format is: 'dns:[//authority/]host[:port]'.")
}

func AddServerConfigParameters(c *cobra.Command) {
	c.PersistentFlags().String("bind-address", "0.0.0.0:11000", "The address to serve on.")
	c.PersistentFlags().Duration("shutdown-timeout", 5*time.Second, "Server shutdown timeout.")
	c.PersistentFlags().String("max-recv-msg-size", "1MB", "Max receive message size.")
	c.PersistentFlags().String("max-send-msg-size", "1MB", "Max send message size.")
	c.PersistentFlags().Uint32("max-concurrent-streams", 1000, "Max number of concurrent streams.")
}

func GetServerConfig(c *cobra.Command) (rpc.ServerConfig, error) {
	bindAddress, err1 := c.Flags().GetString("bind-address")
	shutdownTimeout, err2 := c.Flags().GetDuration("shutdown-timeout")
	if err2 == nil && shutdownTimeout < 0 {
		err2 = errors.Error("shutdown-timeout must be a positive value")
	}
	maxRecvMsgSize, err3 := parseBytes(c, "max-recv-msg-size")
	maxSendMsgSize, err4 := parseBytes(c, "max-send-msg-size")
	maxConcurrentStreams, err5 := c.Flags().GetUint32("max-concurrent-streams")

	err := errors.Join(err1, err2, err3, err4, err5)
	if err != nil {
		return rpc.ServerConfig{}, err
	}

	return rpc.ServerConfig{
		BindAddress:          bindAddress,
		MaxConcurrentStreams: maxConcurrentStreams,
		MaxRecvMsgSize:       maxRecvMsgSize,
		MaxSendMsgSize:       maxSendMsgSize,
		ShutdownTimeout:      shutdownTimeout,
	}, nil
}

func parseBytes(c *cobra.Command, name string) (uint64, error) {
	str, err := c.Flags().GetString(name)
	if err != nil {
		return 0, err
	}

	result, err := humanize.ParseBytes(str)
	if err != nil {
		return 0, errors.Errorf("%s must be a valid byte value", name)
	}

	return result, nil
}

func SetLogLevel(cmd *cobra.Command) {
	str, err := cmd.Flags().GetString("log-level")
	if err != nil {
		logging.Infof(cmd.Context(), "Could not set log level %v", err)
		return
	}

	level, err := logging.ParseLevel(str)
	if err != nil {
		logging.Infof(cmd.Context(), "Invalid log level %q", str)
		return
	}

	logging.SetLevel(level)
}
