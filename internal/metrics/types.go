package metrics

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// Counter records increasing values.
type Counter struct {
	x metric.Int64Counter
}

// Counter records increasing and  decreasing values.
type UpDownCounter struct {
	x metric.Int64UpDownCounter
}

// Gauge records an instantaneous value.
type Gauge struct {
	x metric.Int64Gauge
}

// Histogram records a distribution of values.
type Histogram struct {
	x metric.Int64Histogram
}

func (c Counter) Add(value int) {
	if value < 0 {
		panic("Counter.Add invoked with negative value.")
	}
	c.x.Add(context.Background(), int64(value))
}

func (c UpDownCounter) Add(delta int) {
	c.x.Add(context.Background(), int64(delta))
}

func (c Gauge) Update(value int) {
	c.x.Record(context.Background(), int64(value))
}

func (c Histogram) Record(value int) {
	c.x.Record(context.Background(), int64(value))
}
