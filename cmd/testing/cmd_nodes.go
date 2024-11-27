package main

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/bcrusu/scout/cmd/testing/nodes"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/spf13/cobra"
)

func newNodesCmd() *cobra.Command {
	c := &cobra.Command{
		Use:           "nodes",
		Aliases:       []string{"n"},
		Short:         "Firecracker microVM test nodes.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			workDir, err := getWorkDir(cmd)
			if err != nil {
				return err
			} else if err := ensureEnv(workDir); err != nil {
				return err
			}

			firecrackerPath := path.Join(workDir, "firecracker")

			if err := nodes.InitPIDCache(firecrackerPath); err != nil {
				return err
			}

			return nil
		},
	}

	c.PersistentFlags().String("work-dir", "", "Current dir if not specified.")
	c.PersistentFlags().String("kernel-image", "vmlinux-6.1.102", "Kernel image found in work/downloads dir.")
	c.PersistentFlags().String("kernel-args", "reboot=k panic=1 pci=off", "Kernel args.") // TODO init=/sbin/overlay-init
	c.PersistentFlags().String("rootfs", "rootfs.ext4", "Root filesystem (read-only).")
	c.PersistentFlags().String("workfs", "workfs.ext4", "Work filesystem (read-write).")
	c.PersistentFlags().String("cni-network", "scout_bridge", "CNI network name.")
	c.PersistentFlags().Int("node-cpu", 2, "Node CPU count.")
	c.PersistentFlags().Int("node-memory", 1024, "Node memory size.")

	c.AddCommand(
		newNodesListCmd(),
		newNodesAddCmd(),
		newNodesRemoveCmd(),
		newNodesStartCmd(),
		newNodesStopCmd(),
	)

	return c
}

func GetNodesConfig(c *cobra.Command) (nodes.Config, error) {
	workDir, err := getWorkDir(c)
	if err != nil {
		return nodes.Config{}, err
	}

	nodesDir := path.Join(workDir, "nodes")
	cniBin := path.Join(workDir, "cni_bin")
	cniCache := path.Join(workDir, "cni_cache")
	cniConf := path.Join(workDir, "cni_conf")

	kernelImage := path.Join(workDir, "downloads", errors.Assert2(c.Flags().GetString("kernel-image")))
	rootFS := path.Join(workDir, errors.Assert2(c.Flags().GetString("rootfs")))
	workFS := path.Join(workDir, errors.Assert2(c.Flags().GetString("workfs")))

	nodeCPU := errors.Assert2(c.Flags().GetInt("node-cpu"))
	if nodeCPU < 1 || nodeCPU > 32 {
		return nodes.Config{}, errors.Error("invalid node-cpu value")
	}

	nodeMemory := errors.Assert2(c.Flags().GetInt("node-memory"))
	if nodeMemory < 128 || nodeMemory > 10*1024 {
		return nodes.Config{}, errors.Error("invalid node-memory value")
	}

	if err := utils.MkdirsAll(nodesDir, cniCache); err != nil {
		return nodes.Config{}, err
	}

	if err := pathMustExist(true, workDir, cniBin, cniConf); err != nil {
		return nodes.Config{}, err
	}

	if err := pathMustExist(false, kernelImage, rootFS, workFS); err != nil {
		return nodes.Config{}, err
	}

	return nodes.Config{
		NodesDir:    nodesDir,
		KernelImage: kernelImage,
		KernelArgs:  errors.Assert2(c.Flags().GetString("kernel-args")),
		RootFS:      rootFS,
		WorkFS:      workFS,
		NodeCPU:     nodeCPU,
		NodeMemory:  nodeMemory,
		CNIBin:      cniBin,
		CNIConf:     cniConf,
		CNICache:    cniCache,
		CNINetwork:  errors.Assert2(c.Flags().GetString("cni-network")),
	}, nil
}

func getWorkDir(c *cobra.Command) (string, error) {
	workDir, err := filepath.Abs(errors.Assert2(c.Flags().GetString("work-dir")))
	if err != nil {
		return "", errors.Wrap(err, "failed to determine work dir")
	} else if err := pathMustExist(true, workDir); err != nil {
		return "", err
	}

	return workDir, nil
}

// ensureEnv sets the PATH env variable as expected by firecracker-go-sdk package
func ensureEnv(workDir string) error {
	path := os.Getenv("PATH")
	set := utils.MakeSet(strings.Split(path, ":"))

	if set[workDir] {
		return nil
	}

	var newPath string
	if path == "" {
		newPath = workDir
	} else {
		newPath = path + ":" + workDir
	}

	if err := os.Setenv("PATH", newPath); err != nil {
		return errors.Wrap(err, "failed to set PATH")
	}

	return nil
}

func pathMustExist(isDir bool, paths ...string) error {
	for _, path := range paths {
		stat, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return errors.Wrapf(err, "path %s does not exist", path)
			}
			return errors.Wrapf(err, "could not determine path %s status", path)
		}

		if isDir && !stat.IsDir() {
			return errors.Wrapf(err, "path %s is not a dir", path)
		}
		if !isDir && stat.IsDir() {
			return errors.Wrapf(err, "path %s is not a file", path)
		}
	}

	return nil
}
