package common

// Metrics is an interface that supports updating different kinds of metrics.
// All its functions are thread-safe.
type Metrics interface {
	// GaugeAdd increments a gauge. A gauge is a metric that represents a single
	// numerical value that can arbitrarily go up and down.
	GaugeAdd(name string, value float64)

	// CounterAdd increments a counter. A counter is a metric that represents a
	// single numerical value that only ever goes up.
	CounterAdd(name string, value float64)

	// SummaryObserve adds an observation to a summary. A summary is an aggregate
	// metric that supports querying percentiles.
	SummaryObserve(name string, value float64)
}

// NoOpMetrics is an implementation of Metrics that does nothing.
type NoOpMetrics struct {
}

// GaugeAdd adds the specified value to a gauge of the specified name.
func (n *NoOpMetrics) GaugeAdd(name string, value float64) {
}

// CounterAdd adds the specified value to a counter of the specified name.
// Value should be non-negative.
func (n *NoOpMetrics) CounterAdd(name string, value float64) {
}

// SummaryObserve adds the specified value to a summary of the specified name.
func (n *NoOpMetrics) SummaryObserve(name string, value float64) {
}
