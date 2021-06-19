package main

import (
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	channelDropCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "broadcaster",
		Help:      "Number of dropped channel writes",
		Name:      "channel_drop_total",
	})
	messagesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "broadcaster",
		Help:      "Number of messages sent",
		Name:      "messages_total",
	})
	sseGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem: "broadcaster",
		Help:      "Number of SSE connections",
		Name:      "sse_count",
	})
	webSocketsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem: "broadcaster",
		Help:      "Number of WebSockets connections",
		Name:      "websockets_count",
	})
	dispatchLatencySummary = prometheus.NewSummary(prometheus.SummaryOpts{
		Subsystem: "broadcaster",
		Help:      "Latency of message dispatch",
		Name:      "dispatch_latency_seconds",
	})
	processLatencySummary = prometheus.NewSummary(prometheus.SummaryOpts{
		Subsystem: "broadcaster",
		Help:      "Latency of message processing",
		Name:      "process_latency_seconds",
	})
)

func init() {
	prometheus.MustRegister(channelDropCounter)
	prometheus.MustRegister(messagesCounter)
	prometheus.MustRegister(sseGauge)
	prometheus.MustRegister(webSocketsGauge)
	prometheus.MustRegister(processLatencySummary)
	prometheus.MustRegister(dispatchLatencySummary)

	buildInfoCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Help: "Information about the build",
		Name: "build_info",
		ConstLabels: prometheus.Labels{
			"version":    ProgramVersion,
			"go_version": runtime.Version(),
		},
	})
	prometheus.MustRegister(buildInfoCounter)
	buildInfoCounter.Inc()
}

// PrometheusMetrics is an implementation of broadcaster.Metrics that sends its
// events to Prometheus.
type PrometheusMetrics struct{}

// IncrementWebSocketsCount increments the number of concurrently open WebSockets by delta.
func (*PrometheusMetrics) IncrementWebSocketsCount(delta int) {
	webSocketsGauge.Add(float64(delta))
}

// IncrementSSECount increments the number of concurrently open Server-Side
// Events requests by delta.
func (*PrometheusMetrics) IncrementSSECount(delta int) {
	sseGauge.Add(float64(delta))
}

// IncrementChannelDropCount increases the number of channels that were dropped by one.
func (*PrometheusMetrics) IncrementChannelDropCount() {
	channelDropCounter.Inc()
}

// IncrementMessagesCount increases the number of messages that have been
// processed by one.
func (*PrometheusMetrics) IncrementMessagesCount() {
	messagesCounter.Inc()
}

// ObserveDispatchMessageLatency adds the provided message dispatch latency to
// the summary.
func (*PrometheusMetrics) ObserveDispatchMessageLatency(latency time.Duration) {
	dispatchLatencySummary.Observe(latency.Seconds())
}

// ObserveProcessMessageLatency adds the provided message process latehcy to
// the summary.
func (*PrometheusMetrics) ObserveProcessMessageLatency(latency time.Duration) {
	processLatencySummary.Observe(latency.Seconds())
}
