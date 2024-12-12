package services

import (
	"context"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/testing/nodes"
)

func Configure(ctx context.Context, config Config) error {
	nodes, err := getNodes(ctx, config.SocketPath)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if status, err := getAgentStatus(ctx, node.Ip); err != nil {
			return errors.Wrapf(err, "status check failed for node %s", node.Id)
		} else if status.ServiceType != agent.ServiceType_None {
			return errors.Errorf("node %s is already configured", node.Id)
		}
	}

	configs, err := makeConfigRequests(config, nodes)
	if err != nil {
		return errors.Wrap(err, "failed to make configs")
	}

	for _, node := range nodes {
		if err := configNode(ctx, node, configs[node.Id]); err != nil {
			return err
		}
	}

	return nil
}

func configNode(ctx context.Context, node *nodes.Node, req *agent.ConfigRequest) error {
	client, err := agent.NewClient(node.Ip)
	if err != nil {
		return errors.Wrapf(err, "failed to create agent client for node %s", node.Id)
	}
	defer client.Close()

	if _, err := client.Config(ctx, req); err != nil {
		return errors.Wrapf(err, "failed to configure node %s", node.Id)
	}

	return nil
}

func Start(ctx context.Context, socketPath string) error {
	return doAllNodes(ctx, socketPath, func(client *agent.Client) error {
		_, err := client.Start(ctx, nil)
		return err
	})
}

func Stop(ctx context.Context, socketPath string) error {
	return doAllNodes(ctx, socketPath, func(client *agent.Client) error {
		_, err := client.Stop(ctx, nil)
		return err
	})
}

func Reset(ctx context.Context, socketPath string) error {
	return doAllNodes(ctx, socketPath, func(client *agent.Client) error {
		_, err := client.Reset(ctx, nil)
		return err
	})
}

type nodeAction func(*agent.Client) error

func doAllNodes(ctx context.Context, socketPath string, action nodeAction) error {
	nodeSlice, err := getNodes(ctx, socketPath)
	if err != nil {
		return err
	}

	for _, node := range nodeSlice {
		if node.State != nodes.NodeState_Running {
			continue
		}

		client, err := agent.NewClient(node.Ip)
		if err != nil {
			return errors.Wrapf(err, "failed to create agent client for node %s.", node.Id)
		}

		err = action(client)
		client.Close()

		if err != nil {
			return errors.Wrapf(err, "node %s failed.", node.Id)
		}
	}

	return nil
}

func getNodes(ctx context.Context, socketPath string) ([]*nodes.Node, error) {
	client, err := nodes.NewClient(socketPath)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	resp, err := client.GetNodes(ctx, nil)
	if err != nil {
		return nil, err
	}

	return resp.Nodes, nil
}

func getAgentStatus(ctx context.Context, nodeIP string) (*agent.Status, error) {
	client, err := agent.NewClient(nodeIP)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return client.GetStatus(ctx, nil)
}
