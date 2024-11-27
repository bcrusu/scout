package nodes

import (
	"github.com/bcrusu/scout/internal/utils"
	sdk "github.com/firecracker-microvm/firecracker-go-sdk"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

type Config struct {
	NodesDir    string
	KernelImage string
	KernelArgs  string
	RootFS      string
	WorkFS      string
	NodeCPU     int
	NodeMemory  int
	CNIBin      string
	CNIConf     string
	CNICache    string
	CNINetwork  string
}

func (n Config) GetNodeConfig(node Node) sdk.Config {
	return sdk.Config{
		VMID:            node.ID,
		SocketPath:      node.SocketPath,
		LogPath:         node.LogPath,
		LogLevel:        "Info",
		KernelImagePath: n.KernelImage,
		KernelArgs:      n.KernelArgs,
		MachineCfg: models.MachineConfiguration{
			VcpuCount:       utils.PointerOf(int64(n.NodeCPU)),
			MemSizeMib:      utils.PointerOf(int64(n.NodeMemory)),
			TrackDirtyPages: utils.PointerOf(false),
		},
		Drives: []models.Drive{
			{
				DriveID:      utils.PointerOf("rootfs"),
				IsRootDevice: utils.PointerOf(true),
				IsReadOnly:   utils.PointerOf(true),
				PathOnHost:   utils.PointerOf(n.RootFS),
				IoEngine:     utils.PointerOf("Async"),
			},
			// TODO
			// {
			// 	DriveID:      utils.PointerOf("workfs"),
			// 	IsRootDevice: utils.PointerOf(false),
			// 	IsReadOnly:   utils.PointerOf(false),
			// 	PathOnHost:   utils.PointerOf(n.WorkFS),
			// 	IoEngine:     utils.PointerOf("Async"),
			// },
		},
		NetworkInterfaces: sdk.NetworkInterfaces{
			{
				CNIConfiguration: &sdk.CNIConfiguration{
					NetworkName: n.CNINetwork,
					IfName:      "veth0",
					BinPath:     []string{n.CNIBin},
					ConfDir:     n.CNIConf,
					CacheDir:    n.CNICache,
				},
			},
		},
	}
}
