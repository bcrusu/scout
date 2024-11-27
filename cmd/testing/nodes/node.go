package nodes

import (
	"context"
	"os"
	"path"
	"syscall"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	sdk "github.com/firecracker-microvm/firecracker-go-sdk"
)

const (
	nodeIdPrefix        = "scout"
	apiSocketFileName   = "firecracker.socket"
	logFileName         = "firecracker.log"
	ipFileName          = "ip"
	NodeStateNotStarted = "Not started"
	NodeStateRunning    = "Running"
	NodeStatePaused     = "Paused"
	NodeStateError      = "Error"
)

type Node struct {
	ID         string
	Path       string
	SocketPath string
	LogPath    string
}

func NewNode(nodePath string) Node {
	_, id := path.Split(nodePath)

	return Node{
		ID:         id,
		Path:       nodePath,
		SocketPath: path.Join(nodePath, apiSocketFileName),
		LogPath:    path.Join(nodePath, logFileName),
	}
}

func (n Node) Start(config Config) error {
	state := n.GetState()
	if state == NodeStateRunning {
		return nil
	} else if state != NodeStateNotStarted {
		return errors.Errorf("cannot start node in state %s", state)
	}

	if err := os.Remove(n.SocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Wrap(err, "failed to remove socket file")
	}

	cfg := config.GetNodeConfig(n)
	machine, err := sdk.NewMachine(context.Background(), cfg)
	if err != nil {
		return err
	}

	if err := machine.Start(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start VM")
	}

	ipPath := path.Join(n.Path, ipFileName)
	ip := cfg.NetworkInterfaces[0].StaticConfiguration.IPConfiguration.IPAddr.IP.String()

	if err := os.WriteFile(ipPath, []byte(ip), 0644); err != nil {
		return errors.Wrap(err, "failed to write IP to file")
	}

	return nil
}

func (n Node) Stop() error {
	pid, ok := pidCache[n.ID]
	if !ok {
		return nil
	}

	proc, err := os.FindProcess(int(pid))
	if err != nil {
		return errors.Wrap(err, "find process failed")
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrap(err, "send signal failed")
	}

	return nil
}

func (n Node) GetState() string {
	if exists, err := utils.PathExists(n.SocketPath); err != nil {
		log.WithError(err).Debug("Failed to determine socket state.", "path", n.SocketPath)
		return NodeStateError
	} else if !exists {
		return NodeStateNotStarted
	}

	client := n.newClient()

	info, err := client.GetInstanceInfo(context.Background())
	if err != nil {
		if isConnRefusedErr(err) {
			return NodeStateNotStarted
		}

		log.WithError(err).Debug("Failed to get instance info", "node", n.ID)
		return NodeStateError
	} else if info.Payload == nil || info.Payload.State == nil {
		log.WithError(err).Debug("Instance info payload missing.", "node", n.ID)
		return NodeStateError
	}

	return *info.Payload.State
}

func (n Node) GetPID() int32 {
	return pidCache[n.ID]
}

func (n Node) GetIP() string {
	ipPath := path.Join(n.Path, ipFileName)
	ip, err := os.ReadFile(ipPath)
	if err != nil {
		return ""
	}

	return string(ip)
}

func (n Node) newClient() *sdk.Client {
	return sdk.NewClient(n.SocketPath, nil, false)
}

func isConnRefusedErr(err error) bool {
	serr, ok := errors.As[*os.SyscallError](err)
	if !ok {
		return false
	}

	errno, ok := serr.Err.(syscall.Errno)
	if !ok {
		return false
	}

	return errno.Error() == "connection refused"
}
