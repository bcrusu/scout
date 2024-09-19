package serviceconfig

import (
	"fmt"
	"time"

	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	LBNamePickFirst    = "pick_first"
	LBNameRoundRobin   = "round_robin"
	LBNameGraphControl = "graph_control"
	LBNameGraphData    = "graph_data"
	LBNameGraphApi     = "graph_api"
)

var (
	retryableStatusCodes = []uint32{
		uint32(codes.Canceled),
		uint32(codes.Internal),
		uint32(codes.Unavailable),
	}
	backoffMultiplier = float32(1.75)
)

// Config represents the configuration params for gRPC clients.
type Config struct {
	CallTimeout    time.Duration `yaml:"callTimeout" default:"5s"`
	StreamTimeout  time.Duration `yaml:"streamTimeout" default:"10m"`
	MaxMessageSize utils.Bytes   `yaml:"maxMessageSize" default:"5MB"`
	Retry          Retry         `yaml:"retry"`
}

type Retry struct {
	MaxAttempts uint32        `yaml:"maxAttempts" default:"3"`
	MinDelay    time.Duration `yaml:"minDelay" default:"200ms"`
	MaxDelay    time.Duration `yaml:"maxDelay" default:"500ms"`
}

// GetServiceConfigJson returns the ServiceConfig json for the provided service specification.
func (c Config) GetServiceConfigJson(lbName string, desc grpc.ServiceDesc) string {
	methodNames := make([]*MethodConfig_Name, len(desc.Methods))
	streamNames := make([]*MethodConfig_Name, len(desc.Streams))

	for i, m := range desc.Methods {
		methodNames[i] = &MethodConfig_Name{Service: desc.ServiceName, Method: m.MethodName}
	}

	for i, s := range desc.Streams {
		streamNames[i] = &MethodConfig_Name{Service: desc.ServiceName, Method: s.StreamName}
	}

	sc := &ServiceConfig{
		LoadBalancingConfig: buildLoadBalancingConfig(lbName),
		MethodConfig: []*MethodConfig{
			buildMethodConfig(c, c.CallTimeout, &MethodConfig_Name{}), // matches all methods not listed below
			buildMethodConfig(c, c.CallTimeout, methodNames...),
			buildMethodConfig(c, c.StreamTimeout, streamNames...),
		},
	}
	return sc.ToJson()
}

func DefaultServiceConfig() *ServiceConfig {
	var c Config
	utils.SetDefaults(&c)

	return &ServiceConfig{
		LoadBalancingConfig: buildLoadBalancingConfig(LBNameRoundRobin),
		MethodConfig: []*MethodConfig{
			buildMethodConfig(c, c.CallTimeout, &MethodConfig_Name{}),
		},
	}
}

func buildMethodConfig(c Config, timeout time.Duration, names ...*MethodConfig_Name) *MethodConfig {
	return &MethodConfig{
		Name:                    names,
		WaitForReady:            false,
		Timeout:                 durationpb.New(timeout),
		MaxRequestMessageBytes:  uint32(c.MaxMessageSize.MustParse()),
		MaxResponseMessageBytes: uint32(c.MaxMessageSize.MustParse()),
		RetryPolicy:             buildRetryPolicy(c),
	}
}

func buildRetryPolicy(c Config) *MethodConfig_RetryPolicy {
	return &MethodConfig_RetryPolicy{
		MaxAttempts:          c.Retry.MaxAttempts,
		InitialBackoff:       durationpb.New(c.Retry.MinDelay),
		MaxBackoff:           durationpb.New(c.Retry.MaxDelay),
		BackoffMultiplier:    backoffMultiplier,
		RetryableStatusCodes: retryableStatusCodes,
	}
}

// ToJson returns the json string for the service config
func (c *ServiceConfig) ToJson() string {
	data, err := protojson.Marshal(c)
	if err != nil {
		logging.NoContext().WithError(err).Warnf("Unexpected error when marshal ServiceConfig json")
	}

	return string(data)
}

func buildLoadBalancingConfig(name string) []*LBConfig {
	switch name {
	case LBNamePickFirst:
		return []*LBConfig{{Policy: &LBConfig_RoundRobin{}}}
	case LBNameRoundRobin:
		return []*LBConfig{{Policy: &LBConfig_PickFirst{PickFirst: &LBConfigPickFirst{ShuffleAddressList: true}}}}
	case LBNameGraphControl:
		return []*LBConfig{{Policy: &LBConfig_GraphControl{}}}
	case LBNameGraphData:
		return []*LBConfig{{Policy: &LBConfig_GraphData{}}}
	case LBNameGraphApi:
		return []*LBConfig{{Policy: &LBConfig_GraphApi{}}}
	default:
		panic(fmt.Sprintf("unknown LB %s", name))
	}
}
