package main

import (
	"testing"

	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
)

type fakeMetrics struct {
	gauges    map[string]float64
	counters  map[string]float64
	summaries map[string][]float64
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		gauges:    make(map[string]float64),
		counters:  make(map[string]float64),
		summaries: make(map[string][]float64),
	}
}

func (m *fakeMetrics) GaugeAdd(name string, value float64) {
	m.gauges[name] += value
}

func (m *fakeMetrics) CounterAdd(name string, value float64) {
	m.counters[name] += value
}

func (m *fakeMetrics) SummaryObserve(name string, value float64) {
	m.summaries[name] = append(m.summaries[name], value)
}

func TestQueueEventsProcessorUsesCountersForFailureEvents(t *testing.T) {
	metrics := newFakeMetrics()
	ctx := &grader.Context{
		Context: common.Context{
			Metrics: metrics,
		},
	}

	if previous := globalContext.Load(); previous != nil {
		defer globalContext.Store(previous)
	}
	globalContext.Store(ctx)

	events := make(chan *grader.QueueEvent, 2)
	done := make(chan struct{})
	go func() {
		queueEventsProcessor(events)
		close(done)
	}()

	events <- &grader.QueueEvent{Type: grader.QueueEventTypeRetried}
	events <- &grader.QueueEvent{Type: grader.QueueEventTypeAbandoned}
	close(events)
	<-done

	if got := metrics.counters["grader_runs_retry"]; got != 1 {
		t.Fatalf("retry counter = %v, want 1", got)
	}
	if got := metrics.counters["grader_runs_abandoned"]; got != 1 {
		t.Fatalf("abandoned counter = %v, want 1", got)
	}
	if got := metrics.gauges["grader_runs_retry"]; got != 0 {
		t.Fatalf("retry gauge = %v, want 0", got)
	}
	if got := metrics.gauges["grader_runs_abandoned"]; got != 0 {
		t.Fatalf("abandoned gauge = %v, want 0", got)
	}
}
