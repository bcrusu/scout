package agent

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/durationpb"
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

	if x := ServiceType(value); !ok || !x.IsValid() {
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

	if !x.ServiceType.IsValid() || len(x.ConfigFile) == 0 {
		return errors.Error("ConfigRequest has missing fields")
	}

	return nil
}

func (x *ResetRequest) Validate() error {
	if x == nil {
		return errors.Error("ResetRequest is nil")
	}

	if !x.Time.IsValid() {
		return errors.Error("ResetRequest has invalid fields")
	}

	return nil
}

func (x *NemesisRequest) Validate() error {
	if x == nil {
		return errors.Error("NemesisRequest is nil")
	}

	if x.Payload == nil || !x.Duration.IsValid() || x.Duration.AsDuration() <= time.Millisecond {
		return errors.Error("NemesisRequest has invalid fields")
	}

	return nil
}

func (x *NemesisRequest) Name() string {
	if x.Payload == nil {
		return ""
	}

	name := reflect.TypeOf(x.Payload).String()
	i := strings.LastIndex(name, "_")
	return name[i+1:]
}

func (x *NemesisRequest) Nemesis() Nemesis {
	switch n := x.Payload.(type) {
	case *NemesisRequest_Kill:
		return n.Kill
	case *NemesisRequest_Pause:
		return n.Pause
	case *NemesisRequest_Restart:
		return n.Restart
	case *NemesisRequest_BumpTime:
		return n.BumpTime
	case *NemesisRequest_StrobeTime:
		return n.StrobeTime
	default:
		panic(fmt.Sprintf("Unknown nemesis type %T", x.Nemesis))
	}
}

func (x ServiceType) IsValid() bool {
	return x == ServiceType_Control || x == ServiceType_Data || x == ServiceType_Api
}

func NewNemesisRequest(nemesis Nemesis, duration time.Duration) *NemesisRequest {
	var payload isNemesisRequest_Payload

	switch x := nemesis.(type) {
	case *Kill:
		payload = &NemesisRequest_Kill{Kill: x}
	case *Pause:
		payload = &NemesisRequest_Pause{Pause: x}
	case *Restart:
		payload = &NemesisRequest_Restart{Restart: x}
	case *BumpTime:
		payload = &NemesisRequest_BumpTime{BumpTime: x}
	case *StrobeTime:
		payload = &NemesisRequest_StrobeTime{StrobeTime: x}
	default:
		panic(fmt.Sprintf("Unknown nemesis type %T", nemesis))
	}

	return &NemesisRequest{
		Duration: durationpb.New(duration),
		Payload:  payload,
	}
}
