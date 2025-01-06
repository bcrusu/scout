package agent

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	_ utils.Lifecycle = (*service)(nil)
	_ ServiceServer   = (*service)(nil)
)

type service struct {
	UnsafeServiceServer
	cmdChan      chan any
	cancelFunc   context.CancelFunc
	nemesisCount atomic.Int32
	lock         sync.Mutex
	serviceType  ServiceType
}

type runCmd struct {
	Name        string
	Nemesis     Nemesis
	Duration    time.Duration
	ServiceType ServiceType
}

type stopAllCmd struct {
	DoneCh chan any
}

func newService() *service {
	serviceType, err := readServiceType()
	if err != nil {
		log.WithError(err).Error("Failed to read service type. ")
		serviceType = ServiceType_Unknown
	}

	return &service{
		cmdChan:     make(chan any, 1),
		serviceType: serviceType,
	}
}

func (s *service) Start(ctx context.Context) error {
	s.cancelFunc = utils.RunAsync(ctx, s.mainLoop)
	return nil
}

func (s *service) Stop() {
	s.cancelFunc()
}

func (s *service) GetStatus(ctx context.Context, _ *emptypb.Empty) (*Status, error) {
	// fetch time asap to avoid any unnecessary delays as the value
	// is used to compute max time offset between nodes.
	now := timestamppb.Now()

	s.lock.Lock()
	defer s.lock.Unlock()

	active := false
	var err error
	if s.serviceType.IsValid() {
		active, err = isServiceActive(s.serviceType)
		if err != nil {
			return nil, err
		}
	}

	return &Status{
		ServiceType:   s.serviceType,
		ServiceActive: active,
		Time:          now,
		NemesisCount:  uint32(s.nemesisCount.Load()),
	}, nil
}

func (s *service) ConfigService(ctx context.Context, req *ConfigRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.serviceType.IsValid() {
		return nil, errors.FailedPrecondition
	}

	// just to make sure previous failed attempt files are cleared
	if err := os.RemoveAll(StorageDir); err != nil {
		return nil, errors.Wrap(err, "failed to remove storage dir")
	}

	if err := os.MkdirAll(StorageDir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create storage dir")
	}

	if err := os.WriteFile(ConfigFile, req.ConfigFile, 0644); err != nil {
		return nil, errors.Wrap(err, "failed to write config file")
	}

	if err := utils.EnsureFile(LogFile); err != nil {
		return nil, errors.Wrap(err, "failed to create log file")
	}

	if err := chownPaths(StorageDir, ConfigFile, LogFile); err != nil {
		return nil, err
	}

	if err := writeServiceType(req.ServiceType); err != nil {
		return nil, err
	}

	s.serviceType = req.ServiceType
	return nil, nil
}

func (s *service) StartService(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.serviceType.IsValid() {
		return nil, errors.FailedPrecondition
	}

	if err := startService(s.serviceType); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *service) StopService(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.serviceType.IsValid() {
		return nil, errors.FailedPrecondition
	}

	if err := stopService(s.serviceType); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *service) RestartService(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.serviceType.IsValid() {
		return nil, errors.FailedPrecondition
	}

	if err := restartService(s.serviceType); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *service) ResetService(ctx context.Context, req *ResetRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if req.Nemesis {
		cmd := stopAllCmd{DoneCh: make(chan any)}
		s.cmdChan <- cmd
		<-cmd.DoneCh
	}

	if req.Time != nil {
		if err := utils.SetTime(req.Time.AsTime()); err != nil {
			return nil, err
		}
	}

	if req.TruncateLogs && !req.Service {
		if err := os.Truncate(LogFile, 0); err != nil && !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "failed to truncate log file")
		}
	}

	if !req.Service || s.serviceType == ServiceType_None {
		return nil, nil
	} else if s.serviceType == ServiceType_Unknown {
		if err := stopAllServices(); err != nil {
			return nil, err
		}
	} else if err := stopService(s.serviceType); err != nil {
		return nil, err
	}

	if err := os.RemoveAll(StorageDir); err != nil {
		return nil, errors.Wrap(err, "failed to remove storage dir")
	}

	s.serviceType = ServiceType_None
	return nil, nil
}

func (s *service) GetLogs(ctx context.Context, _ *emptypb.Empty) (*Logs, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.serviceType.IsValid() {
		return &Logs{}, nil
	}

	data, err := os.ReadFile(LogFile)
	if err != nil {
		return nil, err
	}

	return &Logs{Data: data}, nil
}

func (s *service) RunNemesis(ctx context.Context, req *NemesisRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.serviceType.IsValid() {
		return nil, errors.FailedPrecondition
	}

	s.cmdChan <- runCmd{
		Name:        req.Name(),
		Nemesis:     req.Nemesis(),
		Duration:    req.Duration.AsDuration(),
		ServiceType: s.serviceType,
	}

	s.nemesisCount.Add(1)
	return nil, nil
}

func (s *service) mainLoop(parentCtx context.Context) {
	type runInfo struct {
		id      int
		name    string
		nemesis Nemesis
		ctx     context.Context
		cancel  context.CancelFunc
	}

	resultCh := make(chan runInfo)
	run := func(info runInfo) {
		log := log.With("name", info.name, "id", info.id)
		log.Debug("Starting nemesis...")

		if err := info.nemesis.Run(info.ctx); err != nil {
			log.WithError(err).Error("Nemesis failed.")
		} else {
			log.Debug("Nemesis success.")
		}

		resultCh <- info
	}

	running := map[int]runInfo{}
	stopAll := func() {
		for _, info := range running {
			info.cancel()
		}
		for range len(running) {
			<-resultCh
		}
		clear(running)
		s.nemesisCount.Store(0)
	}

	counter := 0

	for {
		select {
		case cmd := <-s.cmdChan:
			switch x := cmd.(type) {
			case runCmd:
				counter++
				ctx, cancel := context.WithTimeout(parentCtx, x.Duration)
				ctx = withServiceType(ctx, x.ServiceType)

				info := runInfo{
					id:      counter,
					name:    x.Name,
					nemesis: x.Nemesis,
					ctx:     ctx,
					cancel:  cancel,
				}

				running[info.id] = info
				go run(info)
			case stopAllCmd:
				stopAll()
				close(x.DoneCh)
			default:
				log.Errorf("Unknown cmd type %T", cmd)
			}
		case info := <-resultCh:
			info.cancel()
			delete(running, info.id)
			s.nemesisCount.Add(-1)
		case <-parentCtx.Done():
			stopAll()
			return
		}
	}
}
