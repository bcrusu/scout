package server

import (
	"github.com/bcrusu/graph/internal/rpc"
)

type Config struct {
	Server      rpc.ServerConfig
	ClusterName string
	DataDir     string
}

type BootstrapConfig struct {
	LocalAddress   string
	InitialServers []string
	PartitionCount uint32
}
