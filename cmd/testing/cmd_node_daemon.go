package main

import (
	"path"
	"syscall"

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
		firecrackerPath := path.Join(workDir, "firecracker")
		nodesDir := path.Join(workDir, "nodes")
		netNSDir := path.Join(workDir, "netns")
		cniBin := path.Join(workDir, "cni_bin")
		cniCache := path.Join(workDir, "cni_cache")
		cniConf := path.Join(workDir, "cni_conf")

		kernelImage := path.Join(workDir, errors.Assert2(c.Flags().GetString("kernel-image")))
		rootFS := path.Join(workDir, errors.Assert2(c.Flags().GetString("rootfs")))
		scoutFS := path.Join(workDir, errors.Assert2(c.Flags().GetString("scoutfs")))
		workFS := path.Join(workDir, errors.Assert2(c.Flags().GetString("workfs")))

		nodeCPU := errors.Assert2(c.Flags().GetInt("node-cpu"))
		if nodeCPU < 1 || nodeCPU > 32 {
			return nodes.Config{}, errors.Error("invalid node-cpu value")
		}

		nodeMemory := errors.Assert2(c.Flags().GetInt("node-memory"))
		if nodeMemory < 128 || nodeMemory > 10*1024 {
			return nodes.Config{}, errors.Error("invalid node-memory value")
		}

		if err := utils.MkdirsAll(nodesDir, netNSDir, cniCache); err != nil {
			return nodes.Config{}, err
		}

		if err := pathMustExist(true, cniBin, cniConf); err != nil {
			return nodes.Config{}, err
		}

		if err := pathMustExist(false, firecrackerPath, kernelImage, rootFS, scoutFS, workFS); err != nil {
			return nodes.Config{}, err
		}

		return nodes.Config{
			SocketPath:      socketPath,
			FirecrackerPath: firecrackerPath,
			NodesDir:        nodesDir,
			KernelImage:     kernelImage,
			KernelArgs:      errors.Assert2(c.Flags().GetString("kernel-args")),
			RootFS:          rootFS,
			ScoutFS:         scoutFS,
			WorkFS:          workFS,
			NodeCPU:         nodeCPU,
			NodeMemory:      nodeMemory,
			NetNSDir:        netNSDir,
			CNIBinDir:       cniBin,
			CNIConfDir:      cniConf,
			CNICacheDir:     cniCache,
			CNINetworkName:  errors.Assert2(c.Flags().GetString("cni-network")),
			LogLevel:        errors.Assert2(c.Flags().GetString("log-level")),
		}, nil
	}

	c := &cobra.Command{
		Use:           "daemon",
		Short:         "Firecracker microVM node manager daemon.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(c *cobra.Command, args []string) error {
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
	c.PersistentFlags().String("kernel-args", "reboot=k panic=1 pci=off", "Kernel args.")
	c.PersistentFlags().String("rootfs", "rootfs.ext4", "Root filesystem (read-only).")
	c.PersistentFlags().String("scoutfs", "scoutfs.ext4", "Scout filesystem (read-only).")
	c.PersistentFlags().String("workfs", "workfs.ext4", "Work filesystem (read-write).")
	c.PersistentFlags().String("cni-network", "scout_bridge", "CNI network name.")
	c.PersistentFlags().Int("node-cpu", 2, "Node CPU count.")
	c.PersistentFlags().Int("node-memory", 1024, "Node memory size.")
	c.PersistentFlags().String("log-level", "Info", "Firecracker VM log level: Error, Warning, Info, Debug (case-sensitive).")

	return c
}

// Esentially, the CNI is the main reason the daemon exists in the first place
// as it requires root to makes changes to the net namespaces and network configs
// during VM creation. This approach enables all the other testing commands to run
// with non-root privileges by calling the daemon to perfom the actual VM actions.
func checkRoot() error {
	uid := syscall.Getuid()
	if uid != 0 {
		return errors.Error("not running as root")
	}
	return nil
}
