package serviceconfig

import (
	"time"

	"github.com/bcrusu/graph/internal/logging"
	"github.com/dustin/go-humanize"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TODO: make configurable
const (
	LBNameGraphControl  = "graph_control"
	LBNameGraphData     = "graph_data"
	LBNameGraphApi      = "graph_api"
	timeout             = 60 * time.Second // TODO: fix timeout for long-lived streams
	maxSendMsgSize      = 4 * humanize.MByte
	maxRecvMsgSize      = 4 * humanize.MByte
	retryMaxAttempts    = 3
	retryInitialBackoff = 300 * time.Millisecond
	retryMaxBackoff     = 5 * retryInitialBackoff
	backoffMultiplier   = 2
)

var (
	retryableStatusCodes = []uint32{
		uint32(codes.Canceled),
		uint32(codes.Internal),
		uint32(codes.Unavailable),
	}
)

// DefaultServiceConfig returns the default service config
func DefaultServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		LoadBalancingConfig: []*LBConfig{
			{Policy: &LBConfig_RoundRobin{}},
		},
		MethodConfig: []*MethodConfig{
			DefaultMethodConfig(),
		},
	}
}

// DefaultConfig returns the default method config
func DefaultMethodConfig() *MethodConfig {
	allMethods := &MethodConfig_Name{}

	return &MethodConfig{
		Name:                    []*MethodConfig_Name{allMethods},
		WaitForReady:            false,
		Timeout:                 durationpb.New(timeout),
		MaxRequestMessageBytes:  maxSendMsgSize,
		MaxResponseMessageBytes: maxRecvMsgSize,
		RetryPolicy:             DefaultRetryPolicy(),
	}
}

// DefaultRetryPolicy returns the default retry policy
func DefaultRetryPolicy() *MethodConfig_RetryPolicy {
	return &MethodConfig_RetryPolicy{
		MaxAttempts:          retryMaxAttempts,
		InitialBackoff:       durationpb.New(retryInitialBackoff),
		MaxBackoff:           durationpb.New(retryMaxBackoff),
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

func (c *ServiceConfig) WithLBRoundRobin() *ServiceConfig {
	c.LoadBalancingConfig = []*LBConfig{
		{Policy: &LBConfig_RoundRobin{}},
	}
	return c
}

func (c *ServiceConfig) WithLBPickFirst(shuffleAddressList bool) *ServiceConfig {
	c.LoadBalancingConfig = []*LBConfig{
		{Policy: &LBConfig_PickFirst{
			PickFirst: &LBConfigPickFirst{ShuffleAddressList: shuffleAddressList},
		}},
	}
	return c
}

func (c *ServiceConfig) WithLBGraphControl() *ServiceConfig {
	c.LoadBalancingConfig = []*LBConfig{
		{Policy: &LBConfig_GraphControl{}},
	}
	return c
}

func (c *ServiceConfig) WithLBGraphData() *ServiceConfig {
	c.LoadBalancingConfig = []*LBConfig{
		{Policy: &LBConfig_GraphData{}},
	}
	return c
}

func (c *ServiceConfig) WithLBGraphApi() *ServiceConfig {
	c.LoadBalancingConfig = []*LBConfig{
		{Policy: &LBConfig_GraphApi{}},
	}
	return c
}
