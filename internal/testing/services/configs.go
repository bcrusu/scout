package services

import (
	aconfig "github.com/bcrusu/scout/internal/api/server/config"
	cconfig "github.com/bcrusu/scout/internal/control/server/config"
	dconfig "github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/discovery"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/bcrusu/scout/internal/validation"
	"gopkg.in/yaml.v3"
)

type Config struct {
	SocketPath    string         `validate:"exists:socket"`
	ClusterName   string         `validate:"required,maxLen:100"`
	ControlNodes  int            `validate:"min:1"`
	ControlConfig cconfig.Config `validate:"skip"`
	DataNodes     int            `validate:"positive"`
	DataConfig    dconfig.Config `validate:"skip"`
	APINodes      int            `validate:"min:1"`
	APIConfig     aconfig.Config `validate:"skip"`
}

type configMap map[string]*agent.ConfigRequest

func makeConfigRequests(config Config, nodes []*nodes.Node) (configMap, error) {
	if err := validation.Validate(config); err != nil {
		return nil, err
	} else if config.ControlConfig.Bootstrap == nil {
		return nil, errors.Error("missing bootstrap config section")
	}

	idx1 := config.ControlNodes
	idx2 := idx1 + config.APINodes
	idx3 := len(nodes)
	if config.DataNodes != 0 {
		idx3 = idx2 + config.DataNodes
	}

	if l := len(nodes); idx1 > l || idx2 > l || idx3 > l {
		return nil, errors.Error("not enough nodes")
	}

	cNodes := nodes[:idx1]
	aNodes := nodes[idx1:idx2]
	dNodes := nodes[idx2:idx3]

	initialServers := makeInitialServers(cNodes)
	discovery := makeDiscovery(cNodes)

	result := configMap{}

	for _, node := range cNodes {
		cfg := config.ControlConfig
		cfg.ClusterName = config.ClusterName
		cfg.DataDir = agent.DataDir
		cfg.Bootstrap.InitialServers = initialServers
		cfg.Register = nil
		cfg.RPC.Address = node.Ip
		cfg.HTTP.Address = node.Ip

		result[node.Id] = &agent.ConfigRequest{
			ServiceType: agent.ServiceType_Control,
			ConfigFile:  marshal(cfg),
		}
	}

	for _, node := range aNodes {
		cfg := config.APIConfig
		cfg.ClusterName = config.ClusterName
		cfg.DataDir = agent.DataDir
		cfg.Discovery = discovery
		cfg.Register.Token = ""
		cfg.Register.Tags = []string{node.Id}
		cfg.RPC.Address = node.Ip
		cfg.HTTP.Address = node.Ip
		cfg.ProxyMode = false

		result[node.Id] = &agent.ConfigRequest{
			ServiceType: agent.ServiceType_Api,
			ConfigFile:  marshal(cfg),
		}
	}

	for _, node := range dNodes {
		cfg := config.DataConfig
		cfg.ClusterName = config.ClusterName
		cfg.DataDir = agent.DataDir
		cfg.Discovery = discovery
		cfg.Register.Token = ""
		cfg.Register.Tags = []string{node.Id}
		cfg.RPC.Address = node.Ip
		cfg.HTTP.Address = node.Ip

		result[node.Id] = &agent.ConfigRequest{
			ServiceType: agent.ServiceType_Data,
			ConfigFile:  marshal(cfg),
		}
	}

	return result, nil
}

func makeInitialServers(nodes []*nodes.Node) []cconfig.InitialServer {
	initialServers := make([]cconfig.InitialServer, len(nodes))
	for i, node := range nodes {
		initialServers[i] = cconfig.InitialServer{
			Address: node.Ip,
			Tags:    []string{node.Id},
		}
	}

	return initialServers
}

func makeDiscovery(nodes []*nodes.Node) discovery.Discovery {
	servers := make([]string, len(nodes))
	for i, node := range nodes {
		servers[i] = node.Ip
	}

	return discovery.Discovery{
		Servers: servers,
	}
}

func marshal(in any) []byte {
	return errors.Assert2(yaml.Marshal(in))
}
