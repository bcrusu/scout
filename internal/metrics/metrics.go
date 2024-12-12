package metrics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/identity"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	scope = "scout"
)

var (
	_   utils.Lifecycle = (*Metrics)(nil)
	log                 = logging.New("metrics")
)

type Config struct {
	ExportEndpoint       string        `yaml:"exportEndpoint"`
	ExportInterval       time.Duration `yaml:"exportInterval" default:"5s" validate:"min:1s"`
	EnableRuntime        bool          `yaml:"enableRuntime" default:"true"`
	ReadMemStatsInterval time.Duration `yaml:"readMemStatsInterval" default:"5s" validate:"min:1s"`
}

type Metrics struct {
	config   Config
	id       identity.Identity
	provider *metric.MeterProvider
}

func New(config Config, id identity.Identity) *Metrics {
	return &Metrics{
		config: config,
		id:     id,
	}
}

func (m *Metrics) Start(ctx context.Context) error {
	if m.config.ExportEndpoint == "" {
		return nil
	}

	resource, err := m.newResource(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create resource")
	}

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(m.config.ExportEndpoint))

	if err != nil {
		return errors.Wrap(err, "failed to create exporter")
	}

	reader := metric.NewPeriodicReader(exporter,
		metric.WithInterval(m.config.ExportInterval))

	provider := metric.NewMeterProvider(
		metric.WithReader(reader),
		metric.WithResource(resource),
	)

	if m.config.EnableRuntime {
		err := runtime.Start(
			runtime.WithMeterProvider(provider),
			runtime.WithMinimumReadMemStatsInterval(m.config.ReadMemStatsInterval))
		if err != nil {
			return errors.Wrap(err, "failed to start runtime instrumentation")
		}
		runtime.Start()
	}

	otel.SetErrorHandler(&errorHandler{})
	otel.SetMeterProvider(provider)
	m.provider = provider
	return nil
}

func (m *Metrics) Stop() {
	if m.config.ExportEndpoint == "" {
		return
	}

	if err := m.provider.ForceFlush(context.Background()); err != nil {
		log.WithError(err).Error("Failed to flush provider.")
	}

	if err := m.provider.Shutdown(context.Background()); err != nil {
		log.WithError(err).Error("Failed to shutdown provider.")
	}
}

func (m *Metrics) newResource(ctx context.Context) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNamespace(scope),
			semconv.ServiceName(fmt.Sprintf("%s/%s", m.id.ClusterName, m.id.ServerName)),
			attribute.String("cluster.name", m.id.ClusterName),
			attribute.String("server.name", m.id.ServerName),
			attribute.String("server.id", strconv.FormatUint(m.id.ServerID, 10)),
			attribute.String("server.type", strings.ToLower(m.id.ServerType.String())),
		),
	)
}
