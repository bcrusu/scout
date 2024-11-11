package metrics

import (
	"context"

	"github.com/bcrusu/scout/internal/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Counter records increasing values.
type Counter struct {
	name string
	x    metric.Int64Counter
}

// Counter records increasing and decreasing values.
type UpDownCounter struct {
	x metric.Int64UpDownCounter
}

// Gauge records an instantaneous value.
type Gauge struct {
	attrs attribute.Set
	x     metric.Int64Gauge
}

// Histogram records a distribution of values.
type Histogram struct {
	x metric.Int64Histogram
}

// NewCounter returns a counter for the provided name.
func NewCounter(name string) Counter {
	return Counter{name, errors.Assert2(getMeter().Int64Counter(name))}
}

// NewUpDownCounter returns a up-down-counter for the provided name.
func NewUpDownCounter(name string) UpDownCounter {
	return UpDownCounter{errors.Assert2(getMeter().Int64UpDownCounter(name))}
}

// NewGauge returns a gauge for the provided name.
func NewGauge(name string) Gauge {
	return Gauge{attribute.Set{}, errors.Assert2(getMeter().Int64Gauge(name))}
}

// NewHistogram returns a histogram for the provided name.
func NewHistogram(name string, bucketBoundaries ...float64) Histogram {
	if len(bucketBoundaries) > 0 {
		return Histogram{errors.Assert2(getMeter().Int64Histogram(name, metric.WithExplicitBucketBoundaries(bucketBoundaries...)))}
	}
	return Histogram{errors.Assert2(getMeter().Int64Histogram(name))}
}

// RegisterCounter registers a Counter.
func RegisterCounter(name string, callback func(Observe)) Unregister {
	meter := getMeter()
	counter := errors.Assert2(meter.Int64ObservableCounter(name))

	reg := errors.Assert2(meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		callback(func(value int, labels ...Labels) {
			if value < 0 {
				log.Warnf("Counter %s invoked with negative value.", name)
				return
			}

			obs.ObserveInt64(counter, int64(value), withAttributeSet(labels))
		})
		return nil
	}, counter))

	return func() {
		if err := reg.Unregister(); err != nil {
			log.WithError(err).Errorf("Failed to unregister Counter %s.", name)
		}
	}
}

// RegisterUpDownCounter registers a UpDownCounter.
func RegisterUpDownCounter(name string, callback func(Observe)) Unregister {
	meter := getMeter()
	counter := errors.Assert2(meter.Int64ObservableUpDownCounter(name))

	reg := errors.Assert2(meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		callback(func(value int, labels ...Labels) {
			obs.ObserveInt64(counter, int64(value), withAttributeSet(labels))
		})
		return nil
	}, counter))

	return func() {
		if err := reg.Unregister(); err != nil {
			log.WithError(err).Errorf("Failed to unregister UpDownCounter %s.", name)
		}
	}
}

// RegisterGauge registers a Gauge.
func RegisterGauge(name string, callback func(Observe)) Unregister {
	meter := getMeter()
	gauge := errors.Assert2(meter.Int64ObservableGauge(name))

	reg := errors.Assert2(meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		callback(func(value int, labels ...Labels) {
			obs.ObserveInt64(gauge, int64(value), withAttributeSet(labels))
		})
		return nil
	}, gauge))

	return func() {
		if err := reg.Unregister(); err != nil {
			log.WithError(err).Errorf("Failed to unregister Gauge %s.", name)
		}
	}
}

func getMeter() metric.Meter {
	// will be able to add meter-scoped attributes usingWithInstrumentationAttributes()
	// once https://github.com/open-telemetry/opentelemetry-go/issues/5846 is merged
	return otel.Meter(scope)
}

func (c Counter) Add(value int, labels ...Labels) {
	if value < 0 {
		log.Warnf("Counter %s invoked with negative value.", c.name)
		return
	}

	c.x.Add(context.Background(), int64(value), withAttributeSet(labels))
}

func (c UpDownCounter) Add(delta int, labels ...Labels) {
	c.x.Add(context.Background(), int64(delta), withAttributeSet(labels))
}

func (c Gauge) Update(value int, labels ...Labels) {
	c.x.Record(context.Background(), int64(value), withAttributeSet(labels))
}

func (c Histogram) Record(value int, labels ...Labels) {
	c.x.Record(context.Background(), int64(value), withAttributeSet(labels))
}

func withAttributeSet(labels []Labels) metric.MeasurementOption {
	return metric.WithAttributeSet(mergeLabels(labels))
}
