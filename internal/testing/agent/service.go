package agent

import (
	"context"
	"os"
	"sync"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	_ ServiceServer = (*service)(nil)
)

type service struct {
	UnsafeServiceServer
	lock        sync.Mutex
	serviceType ServiceType
}

func newService() *service {
	serviceType, err := readServiceType()
	if err != nil {
		log.WithError(err).Error("Failed to read service type. ")
		serviceType = ServiceType_Unknown
	}

	return &service{
		serviceType: serviceType,
	}
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
	}, nil
}

func (s *service) Config(ctx context.Context, req *ConfigRequest) (*emptypb.Empty, error) {
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

func (s *service) Start(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
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

func (s *service) Stop(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
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

func (s *service) Restart(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
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

func (s *service) Reset(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.serviceType == ServiceType_None {
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
