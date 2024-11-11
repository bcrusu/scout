package metrics

import (
	"sync/atomic"
)

// In OpenTelemetry, as far as I can gather, the only way to create a metric that can
// be later removed (i.e. unregistered) is to use the RegisterCallback mechanism
// which returns to the caller an Registration object with the Unregister() method.
// All the other ways of creating metrics do not allow removal and will continue to
// forever emit the last value observed.
//
// Removing metrics so they stop emitting values to the time series db is necessary in
// a system with dynamic lifecycle components. For example, the components associated
// with the Raft leader are created when the node takes leadership and then destroyed
// when leader status is lost and the metrics associated with the leader need to stop
// emitting from the old node.
//
// The entire observable/asynchronous instrument (OpenTelemetry's fancy word for metric)
// and the RegisterCallback approach feels backwards when compared with the classic
// Dropwizard-style metrics where things are as simple as:
//   - create a metric: myCounter := NewCounter("my.beautiful.counter")
//   - register it: registry.Register(myCounter)
//   - use the metric for a while, and then later, when we are done,
//   - unregister it: registry.Unregister(myCounter)
//   - no drama, plain, out-of-your-way, boring code that just works...
//
// And on top of this, OpenTelemetry seems to only allow counters and gauges to be
// unregistred using the above, while histograms will live forever...

type Observe func(value int, labels ...Labels)

type Unregister func()

func ObserveAtomicInt64(name string, a *atomic.Int64, labels ...Labels) Unregister {
	return RegisterGauge(name, func(observe Observe) {
		observe(int(a.Load()), labels...)
	})
}

// UnregisterAll is a helper function to unregister multiple metrics.
func UnregisterAll(all ...Unregister) Unregister {
	return func() {
		for _, unregister := range all {
			unregister()
		}
	}
}
