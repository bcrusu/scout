package nodes

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	sdk "github.com/firecracker-microvm/firecracker-go-sdk"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/models"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ ServiceServer = (*service)(nil)
)

type service struct {
	UnsafeServiceServer
	config  Config
	lock    sync.Mutex
	lastId  int
	pids    map[string]int         // map[node_ID]PID
	clients map[string]*sdk.Client // map[node_ID]Client
}

func newService(config Config) (*service, error) {
	pids, err := loadProcs(config.FirecrackerPath)
	if err != nil {
		return nil, err
	}

	lastId, err := utils.GetLastSuffix(config.NodesDir, nodeIdPrefix)
	if err != nil {
		return nil, err
	}

	return &service{
		config:  config,
		lastId:  lastId,
		pids:    pids,
		clients: map[string]*sdk.Client{},
	}, nil
}

func (s *service) GetNode(_ context.Context, id *Id) (*Node, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.getNode(id.Id)
}

func (s *service) GetNodes(_ context.Context, _ *emptypb.Empty) (*Nodes, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	result, err := s.getAllNodes()
	if err != nil {
		return nil, err
	}

	return &Nodes{
		Nodes: result,
	}, nil
}

func (s *service) Create(_ context.Context, req *CreateRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if req.Count < 1 || req.Count > 10 {
		return nil, errors.InvalidRequest
	}

	for range req.Count {
		s.lastId++
		id := fmt.Sprintf("%s%03d", nodeIdPrefix, s.lastId)
		nodeDir := s.getNodeDir(id)

		if exists, err := utils.PathExists(nodeDir); err != nil {
			return nil, errors.Wrapf(err, "could not determine node dir %s status", nodeDir)
		} else if exists {
			return nil, errors.Wrapf(err, "node dir %s already exists", nodeDir)
		}

		if err := utils.MkdirAll(nodeDir); err != nil {
			return nil, err
		}

		if err := s.copyWorkFS(id); err != nil {
			return nil, err
		}

		log.Info("Success.", "action", "create", "node", id)
	}

	return nil, nil
}

func (s *service) Start(_ context.Context, req *Ids) (*Status, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes, err := s.filterNodes(req)
	if err != nil {
		return nil, err
	}

	failures := 0
	for _, node := range nodes {
		if s.getVMState(node.Id) == NodeState_Running {
			continue
		}

		if err := s.startNode(node.Id); err != nil {
			log.WithError(err).Error("Start failed.", "node", node.Id)
			failures++
		} else {
			log.Info("Start success.", "node", node.Id)
		}
	}

	return &Status{FailureCount: int32(failures)}, nil
}

func (s *service) Stop(_ context.Context, req *Ids) (*Status, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes, err := s.filterNodes(req)
	if err != nil {
		return nil, err
	}

	failures := 0
	for _, node := range nodes {
		if s.getVMState(node.Id) == NodeState_NotStarted {
			continue
		}

		if err := s.stopNode(node.Id); err != nil {
			log.WithError(err).Error("Stop failed.", "node", node.Id)
			failures++
		} else {
			log.Info("Stop success.", "node", node.Id)
		}
	}

	return &Status{FailureCount: int32(failures)}, nil
}

func (s *service) Reset(_ context.Context, req *Ids) (*Status, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes, err := s.filterNodes(req)
	if err != nil {
		return nil, err
	}

	failures := 0
	for _, node := range nodes {
		if err := s.resetNode(node.Id); err != nil {
			log.WithError(err).Error("Reset failed.", "node", node.Id)
			failures++
		} else {
			log.Info("Reset success.", "node", node.Id)
		}
	}

	return &Status{FailureCount: int32(failures)}, nil
}

func (s *service) Remove(_ context.Context, req *Ids) (*Status, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes, err := s.filterNodes(req)
	if err != nil {
		return nil, err
	}

	failures := 0
	for _, node := range nodes {
		if err := s.removeNode(node.Id); err != nil {
			log.WithError(err).Error("Remove failed.", "node", node.Id)
			failures++
		} else {
			log.Info("Remove success.", "node", node.Id)
		}
	}

	return &Status{FailureCount: int32(failures)}, nil
}

func (s *service) getAllNodes() ([]*Node, error) {
	entries, err := os.ReadDir(s.config.NodesDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list nodes dir")
	}

	result := []*Node{}

	for _, entry := range entries {
		id := entry.Name()

		if !entry.IsDir() || !strings.HasPrefix(id, nodeIdPrefix) {
			continue
		}

		node, err := s.getNode(id)
		if err != nil {
			return nil, err
		}

		result = append(result, node)
	}

	slices.SortFunc(result, func(a, b *Node) int { return strings.Compare(a.Id, b.Id) })
	return result, nil
}

func (s *service) filterNodes(req *Ids) ([]*Node, error) {
	nodes, err := s.getAllNodes()
	if err != nil {
		return nil, err
	}

	// select all when request ids is empty
	if req == nil || len(req.Ids) == 0 {
		return nodes, nil
	}

	set := utils.MakeSet(req.Ids)
	var result []*Node

	for _, node := range nodes {
		if set[node.Id] {
			result = append(result, node)
		}
	}

	return result, nil
}

func (s *service) getNode(id string) (*Node, error) {
	nodeDir := s.getNodeDir(id)
	if ok, err := utils.PathExists(nodeDir); err != nil {
		return nil, err
	} else if !ok {
		return nil, errors.NotFound
	}

	var err error
	state := s.getVMState(id)
	pid := 0
	ip := ""

	if state == NodeState_Running {
		pid = s.pids[id]

		ip, err = s.readIP(id)
		if err != nil {
			return nil, err
		}
	}

	return &Node{
		Id:    id,
		Pid:   uint32(pid),
		Ip:    ip,
		State: state,
	}, nil
}

func (s *service) startNode(id string) error {
	if s.getVMState(id) == NodeState_Running {
		return nil
	}

	if err := os.Remove(s.getNetNSFile(id)); err != nil && !os.IsNotExist(err) {
		log.WithError(err).Debug("Failed to remove old net namespace file.")
	}

	cfg, err := s.makeVMConfig(id)
	if err != nil {
		return errors.Wrap(err, "failed to make VM config")
	}

	socketPath := s.getNodeSocketFile(id)
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Wrap(err, "failed to remove old socket file")
	}

	if err := utils.EnsureFile(cfg.LogPath); err != nil {
		return err
	}

	// Setting the log-path arg avoids any stdout output during VM init.
	// Avoid setting the cmd.Stdin to os.Stdin which captures daemon input.
	cmd := exec.CommandContext(
		context.Background(),
		s.config.FirecrackerPath,
		"--api-sock", socketPath, "--id", id, "--log-path", cfg.LogPath, "--level", cfg.LogLevel)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	machine, err := sdk.NewMachine(context.Background(), cfg,
		sdk.WithProcessRunner(cmd),
		sdk.WithLogger(newLogger(id, false)))

	if err != nil {
		return errors.Wrap(err, "NewMachine call failed")
	}

	if err := machine.Start(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start VM")
	}

	ipFile := s.getNodeIPFile(id)
	ip := cfg.NetworkInterfaces[0].StaticConfiguration.IPConfiguration.IPAddr.IP.String()

	if err := os.WriteFile(ipFile, []byte(ip), 0644); err != nil {
		return errors.Wrap(err, "failed to write IP to file")
	}

	pid, _ := machine.PID()
	s.pids[id] = pid
	return nil
}

func (s *service) stopNode(id string) error {
	if s.getVMState(id) != NodeState_Running {
		return nil
	}

	pid, ok := s.pids[id]
	if !ok {
		return nil
	}

	proc, err := os.FindProcess(pid)
	if err == nil {
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			return errors.Wrap(err, "send stop signal failed")
		}
	}

	delete(s.pids, id)
	delete(s.clients, id)
	return nil
}

func (s *service) resetNode(id string) error {
	if err := s.stopNode(id); err != nil {
		return err
	}

	workFSFile := s.getNodeWorkFSFile(id)
	if err := os.Remove(workFSFile); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to remove work filesystem")
	}

	return s.copyWorkFS(id)
}

func (s *service) removeNode(id string) error {
	if err := s.stopNode(id); err != nil {
		return err
	}

	nodeDir := s.getNodeDir(id)
	if err := os.RemoveAll(nodeDir); err != nil {
		return errors.Wrap(err, "failed to remove node dir")
	}

	return nil
}

func (s *service) getVMState(id string) NodeState {
	socketPath := s.getNodeSocketFile(id)
	if ok, err := utils.PathExists(socketPath); err != nil {
		log.WithError(err).Error("Failed to determine socket stats.", "node", id)
		return NodeState_Error
	} else if !ok {
		return NodeState_NotStarted
	}

	client := s.getClient(id, socketPath)

	info, err := client.GetInstanceInfo(context.Background())
	if err != nil {
		if s.isConnRefusedErr(err) {
			return NodeState_NotStarted
		}

		log.WithError(err).Error("Failed to get instance info.", "node", id)
		return NodeState_Error
	} else if info.Payload == nil || info.Payload.State == nil {
		log.WithError(err).Error("Instance info payload missing.", "node", id)
		return NodeState_Error
	}

	switch *info.Payload.State {
	case models.InstanceInfoStateNotStarted:
		return NodeState_NotStarted
	case models.InstanceInfoStateRunning:
		return NodeState_Running
	case models.InstanceInfoStatePaused:
		return NodeState_Paused
	default:
		log.WithError(err).Error("Unknown VM state.", "node", id)
		return NodeState_Error
	}
}

func (s *service) getClient(id, socketPath string) *sdk.Client {
	client, ok := s.clients[id]
	if !ok {
		logger := newLogger(id, true)
		client = sdk.NewClient(socketPath, logger, false)
		s.clients[id] = client
	}

	return client
}

func (s *service) readIP(id string) (string, error) {
	ipFile := s.getNodeIPFile(id)
	ip, err := os.ReadFile(ipFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", errors.NotFound
		}
		return "", errors.Wrap(err, "failed to read IP file")
	}

	return string(ip), nil
}

func (s *service) isConnRefusedErr(err error) bool {
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

func (s *service) copyWorkFS(id string) error {
	dest := s.getNodeWorkFSFile(id)
	cmd := exec.Command("cp", "--sparse=always", s.config.WorkFS, dest)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to copy work filesystem to %s", dest)
	}

	return nil
}

func (s *service) getNodeDir(id string) string {
	return path.Join(s.config.NodesDir, id)
}

func (s *service) getNodeSocketFile(id string) string {
	return path.Join(s.config.NodesDir, id, apiSocketFileName)
}

func (s *service) getNodeWorkFSFile(id string) string {
	return path.Join(s.config.NodesDir, id, workFSFileName)
}

func (s *service) getNodeIPFile(id string) string {
	return path.Join(s.config.NodesDir, id, ipFileName)
}

func (s *service) getNodeLogFile(id string) string {
	return path.Join(s.config.NodesDir, id, logFileName)
}

func (s *service) getNetNSFile(id string) string {
	return path.Join(s.config.NetNSDir, id)
}

func (s *service) makeVMConfig(id string) (sdk.Config, error) {
	cfg := sdk.Config{
		VMID:            id,
		SocketPath:      s.getNodeSocketFile(id),
		LogPath:         s.getNodeLogFile(id),
		LogLevel:        s.config.LogLevel,
		KernelImagePath: s.config.KernelImage,
		KernelArgs:      fmt.Sprintf("%s init=/sbin/overlay-init scout_hostname=%s", s.config.KernelArgs, id),
		MachineCfg: models.MachineConfiguration{
			VcpuCount:       utils.PointerOf(int64(s.config.NodeCPU)),
			MemSizeMib:      utils.PointerOf(int64(s.config.NodeMemory)),
			TrackDirtyPages: utils.PointerOf(false),
			Smt:             utils.PointerOf(true),
		},
		Drives: []models.Drive{
			{
				DriveID:      utils.PointerOf("rootfs"),
				IsRootDevice: utils.PointerOf(true),
				IsReadOnly:   utils.PointerOf(true),
				PathOnHost:   utils.PointerOf(s.config.RootFS),
				IoEngine:     utils.PointerOf("Async"),
			},
			{
				DriveID:      utils.PointerOf("scoutfs"),
				IsRootDevice: utils.PointerOf(false),
				IsReadOnly:   utils.PointerOf(true),
				PathOnHost:   utils.PointerOf(s.config.ScoutFS),
				IoEngine:     utils.PointerOf("Async"),
			},
			{
				DriveID:      utils.PointerOf("workfs"),
				IsRootDevice: utils.PointerOf(false),
				IsReadOnly:   utils.PointerOf(false),
				PathOnHost:   utils.PointerOf(s.getNodeWorkFSFile(id)),
				IoEngine:     utils.PointerOf("Async"),
			},
		},
		NetNS: s.getNetNSFile(id),
		NetworkInterfaces: sdk.NetworkInterfaces{
			{
				CNIConfiguration: &sdk.CNIConfiguration{
					NetworkName: s.config.CNINetworkName,
					IfName:      "veth0",
					BinPath:     []string{s.config.CNIBinDir},
					ConfDir:     s.config.CNIConfDir,
					CacheDir:    s.config.CNICacheDir,
				},
			},
		},
		ForwardSignals: []os.Signal{}, // do not forward default signals
	}

	ip, err := s.readIP(id)
	if err != nil && !errors.Is(err, errors.NotFound) {
		return sdk.Config{}, err
	}

	if ip != "" {
		cfg.NetworkInterfaces[0].CNIConfiguration.Args = [][2]string{
			{"IgnoreUnknown", "true"},
			{"IP", ip},
		}
	}

	return cfg, nil
}
