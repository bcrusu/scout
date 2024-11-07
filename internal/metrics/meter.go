package metrics

import (
	"context"

	"github.com/bcrusu/scout/internal/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	meterScope = "scout"
)

type PullMeter struct {
	Name string
	Pull func() int64
}

type Unregister func()

func getMeter() metric.Meter {
	// will be able to add attributes using metric.WithInstrumentationAttributes() once
	// https://github.com/open-telemetry/opentelemetry-go/issues/5846 is merged
	return otel.Meter(meterScope)
}

// NewCounter returns a counter for the provided name.
func NewCounter(name string) Counter {
	return Counter{errors.Assert2(getMeter().Int64Counter(name))}
}

// NewUpDownCounter returns a up-down-counter for the provided name.
func NewUpDownCounter(name string) UpDownCounter {
	return UpDownCounter{errors.Assert2(getMeter().Int64UpDownCounter(name))}
}

// NewGauge returns a gauge for the provided name.
func NewGauge(name string) Gauge {
	return Gauge{errors.Assert2(getMeter().Int64Gauge(name))}
}

// NewHistogram returns a histogram for the provided name.
func NewHistogram(name string) Histogram {
	return Histogram{errors.Assert2(getMeter().Int64Histogram(name))}
}

// RegisterPull registers for meter values to be periodically pulled.
func RegisterPull(counters, gauges []PullMeter) Unregister {
	if len(counters) == 0 && len(gauges) == 0 {
		return func() {}
	}

	meter := getMeter()
	all := map[string]metric.Int64Observable{}

	for _, c := range counters {
		if _, ok := all[c.Name]; ok {
			log.Warnf("Duplicate pull meter %s name.", c.Name)
			continue
		}
		all[c.Name] = errors.Assert2(meter.Int64ObservableUpDownCounter(c.Name))
	}

	for _, c := range gauges {
		if _, ok := all[c.Name]; ok {
			log.Warnf("Duplicate pull meter %s name.", c.Name)
			continue
		}
		all[c.Name] = errors.Assert2(meter.Int64ObservableGauge(c.Name))
	}

	slice := make([]metric.Observable, 0, len(all))
	for _, obs := range all {
		slice = append(slice, obs)
	}

	reg := errors.Assert2(meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		for _, c := range counters {
			obs.ObserveInt64(all[c.Name], c.Pull())
		}

		for _, g := range gauges {
			obs.ObserveInt64(all[g.Name], g.Pull())
		}

		return nil
	}, slice...))

	return func() {
		if err := reg.Unregister(); err != nil {
			log.WithError(err).Error("Failed to unregister pull meters.")
		}
	}
}
