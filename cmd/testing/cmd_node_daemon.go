package main

import (
	"path"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/testing/nodes"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newNodeDaemonCmd() *cobra.Command {
	getConfig := func(c *cobra.Command) (nodes.Config, error) {
		workDir, err := getWorkDir(c)
		if err != nil {
			return nodes.Config{}, err
		}

		socketPath := joinSocketPath(workDir)
		nodesDir := path.Join(workDir, "nodes")
		netNSDir := path.Join(workDir, "netns")
		cniCache := path.Join(workDir, "cni_cache")

		if err := utils.MkdirsAll(nodesDir, netNSDir, cniCache); err != nil {
			return nodes.Config{}, err
		}

		return nodes.Config{
			SocketPath:      socketPath,
			FirecrackerPath: path.Join(workDir, "firecracker"),
			NodesDir:        nodesDir,
			KernelImage:     path.Join(workDir, errors.Assert2(c.Flags().GetString("kernel-image"))),
			KernelArgs:      errors.Assert2(c.Flags().GetString("kernel-args")),
			RootFS:          path.Join(workDir, errors.Assert2(c.Flags().GetString("rootfs"))),
			ScoutFS:         path.Join(workDir, errors.Assert2(c.Flags().GetString("scoutfs"))),
			WorkFS:          path.Join(workDir, errors.Assert2(c.Flags().GetString("workfs"))),
			NodeCPU:         errors.Assert2(c.Flags().GetInt("node-cpu")),
			NodeMemory:      errors.Assert2(c.Flags().GetInt("node-memory")),
			NetNSDir:        netNSDir,
			CNIBinDir:       path.Join(workDir, "cni_bin"),
			CNIConfDir:      path.Join(workDir, "cni_conf"),
			CNICacheDir:     cniCache,
			CNINetworkName:  errors.Assert2(c.Flags().GetString("cni-network")),
			LogLevel:        errors.Assert2(c.Flags().GetString("log-level")),
		}, nil
	}

	c := &cobra.Command{
		Use:           "daemon",
		Aliases:       []string{"d"},
		Short:         "Firecracker microVM node manager daemon.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
			// Esentially, the CNI is the main reason the daemon exists in the first place
			// as it requires root to makes changes to the net namespaces and network configs
			// during VM creation. This approach enables all the other testing commands to run
			// with non-root privileges by calling the daemon to perfom the actual VM actions.
			if err := checkRoot(); err != nil {
				return err
			}

			config, err := getConfig(c)
			if err != nil {
				return err
			}

			log := logging.New("daemon")
			server := nodes.NewServer(config)
			return utils.LifecycleRun(c.Context(), log, server)
		},
	}

	c.PersistentFlags().String("kernel-image", "downloads/vmlinux-6.1.102", "Kernel image.")
	c.PersistentFlags().String("kernel-args", "ro nomodule reboot=k panic=1 pci=off clocksource=kvm-clock", "Kernel args.")
	c.PersistentFlags().String("rootfs", "rootfs.ext4", "Root filesystem (read-only).")
	c.PersistentFlags().String("scoutfs", "scoutfs.ext4", "Scout filesystem (read-only).")
	c.PersistentFlags().String("workfs", "workfs.ext4", "Work filesystem (read-write).")
	c.PersistentFlags().String("cni-network", "scout_bridge", "CNI network name.")
	c.PersistentFlags().Int("node-cpu", 2, "Node CPU count.")
	c.PersistentFlags().Int("node-memory", 1024, "Node memory size.")
	c.PersistentFlags().String("log-level", "Info", "Firecracker VM log level: Error, Warning, Info, Debug (case-sensitive).")

	return c
}
