package agent

import (
	"os"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
)

const (
	Port            = 9909
	StorageDir      = "/storage"
	DataDir         = StorageDir + "/data"
	ConfigFile      = StorageDir + "/config.yaml"
	LogFile         = StorageDir + "/scout.log"
	ServiceTypeFile = StorageDir + "/service_type"
	UID             = 888
	GID             = 888
)

var (
	log = logging.New("agent")
)

func readServiceType() (ServiceType, error) {
	data, err := os.ReadFile(ServiceTypeFile)
	if err != nil {
		if os.IsNotExist(err) {
			return ServiceType_None, nil
		}
		return 0, errors.Wrap(err, "failed to read service type")
	}

	value, ok := ServiceType_value[string(data)]

	if !ok || value == int32(ServiceType_None) {
		return 0, errors.Error("found invalid service")
	}

	return ServiceType(value), nil
}

func writeServiceType(value ServiceType) error {
	if exists, err := utils.PathExists(ServiceTypeFile); err != nil {
		return err
	} else if exists {
		return errors.Error("cannot overwrite service type")
	}

	data := []byte(value.String())
	if err := os.WriteFile(ServiceTypeFile, data, 0644); err != nil {
		return errors.Wrap(err, "failed to write service type")
	}

	return nil
}

func chownPaths(paths ...string) error {
	for _, path := range paths {
		if err := os.Chown(path, UID, GID); err != nil {
			return errors.Wrapf(err, "failed to chown %s", path)
		}
	}
	return nil
}

func (x *ConfigRequest) Validate() error {
	if x == nil {
		return errors.Error("ConfigRequest is nil")
	}

	if x.ServiceType == ServiceType_None || len(x.ConfigFile) == 0 {
		return errors.Error("ConfigRequest has missing fields")
	}

	if _, ok := ServiceType_name[int32(x.ServiceType)]; !ok {
		return errors.Error("ConfigRequest.ServiceType is invalid")
	}

	return nil
}
