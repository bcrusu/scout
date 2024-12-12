package agent

import (
	"context"
	"os"
	"sync"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ ServiceServer = (*service)(nil)
)

type service struct {
	UnsafeServiceServer
	lock        sync.Mutex
	serviceType ServiceType
}

func newService() (*service, error) {
	serviceType, err := readServiceType()
	if err != nil {
		return nil, err
	}

	return &service{
		serviceType: serviceType,
	}, nil
}

func (s *service) GetStatus(ctx context.Context, _ *emptypb.Empty) (*Status, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	active := false
	var err error
	if s.serviceType != ServiceType_None {
		active, err = isServiceActive(ctx, s.serviceType)
		if err != nil {
			return nil, err
		}
	}

	return &Status{
		ServiceType:   s.serviceType,
		ServiceActive: active,
	}, nil
}

func (s *service) Config(ctx context.Context, req *ConfigRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.serviceType != ServiceType_None {
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

	if s.serviceType == ServiceType_None {
		return nil, errors.FailedPrecondition
	}

	if err := startService(ctx, s.serviceType); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *service) Stop(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.serviceType == ServiceType_None {
		return nil, errors.FailedPrecondition
	}

	if err := stopService(ctx, s.serviceType); err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *service) Reset(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.serviceType == ServiceType_None {
		return nil, nil
	}

	if err := stopService(ctx, s.serviceType); err != nil {
		return nil, err
	}

	if err := os.RemoveAll(StorageDir); err != nil {
		return nil, errors.Wrap(err, "failed to remove storage dir")
	}

	s.serviceType = ServiceType_None
	return nil, nil
}
