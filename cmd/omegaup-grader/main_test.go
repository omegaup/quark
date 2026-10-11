package main

import (
	"testing"
	"time"

	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
)

type recordedMetric struct {
	name  string
	value float64
}

type recordingMetrics struct {
	gaugeAdds           []recordedMetric
	counterAdds         []recordedMetric
	summaryObservations []recordedMetric
}

func (m *recordingMetrics) GaugeAdd(name string, value float64) {
	m.gaugeAdds = append(m.gaugeAdds, recordedMetric{name: name, value: value})
}

func (m *recordingMetrics) CounterAdd(name string, value float64) {
	m.counterAdds = append(m.counterAdds, recordedMetric{name: name, value: value})
}

func (m *recordingMetrics) SummaryObserve(name string, value float64) {
	m.summaryObservations = append(m.summaryObservations, recordedMetric{name: name, value: value})
}

func (m *recordingMetrics) findSummaryObservation(name string) (float64, bool) {
	for _, observation := range m.summaryObservations {
		if observation.name == name {
			return observation.value, true
		}
	}
	return 0, false
}

func (m *recordingMetrics) gaugeTotal(name string) float64 {
	var total float64
	for _, observation := range m.gaugeAdds {
		if observation.name == name {
			total += observation.value
		}
	}
	return total
}

func feedQueueEvents(metrics *recordingMetrics, events ...*grader.QueueEvent) {
	globalContext.Store(&grader.Context{
		Context: common.Context{
			Metrics: metrics,
		},
	})

	ch := make(chan *grader.QueueEvent, len(events))
	for _, ev := range events {
		ch <- ev
	}
	close(ch)

	queueEventsProcessor(ch)
}

func TestQueueEventsProcessorObservesTimeToVerdictOnManagerRemoved(t *testing.T) {
	metrics := &recordingMetrics{}
	globalContext.Store(&grader.Context{
		Context: common.Context{
			Metrics: metrics,
		},
	})

	events := make(chan *grader.QueueEvent, 1)
	events <- &grader.QueueEvent{
		Type:  grader.QueueEventTypeManagerRemoved,
		Delta: 2 * time.Second,
	}
	close(events)

	queueEventsProcessor(events)

	value, ok := metrics.findSummaryObservation("grader_time_to_verdict_seconds")
	if !ok {
		t.Fatal("missing summary observation for grader_time_to_verdict_seconds")
	}
	if value != 2 {
		t.Errorf("summary observation for grader_time_to_verdict_seconds = %f, want 2", value)
	}
	if _, observed := metrics.findSummaryObservation("grader_queue_delay_seconds"); observed {
		t.Fatal("grader_queue_delay_seconds must not be observed on ManagerRemoved")
	}
}

func TestQueueEventsProcessorObservesQueueDelayOnQueueRemoved(t *testing.T) {
	metrics := &recordingMetrics{}
	globalContext.Store(&grader.Context{
		Context: common.Context{
			Metrics: metrics,
		},
	})

	events := make(chan *grader.QueueEvent, 1)
	events <- &grader.QueueEvent{
		Type:     grader.QueueEventTypeQueueRemoved,
		Priority: grader.QueuePriorityNormal,
		Delta:    2 * time.Second,
	}
	close(events)

	queueEventsProcessor(events)

	value, ok := metrics.findSummaryObservation("grader_queue_delay_seconds")
	if !ok {
		t.Fatal("missing summary observation for grader_queue_delay_seconds")
	}
	if value != 2 {
		t.Errorf("summary observation for grader_queue_delay_seconds = %f, want 2", value)
	}
	if _, observed := metrics.findSummaryObservation("grader_time_to_verdict_seconds"); observed {
		t.Fatal("grader_time_to_verdict_seconds must not be observed on QueueRemoved")
	}
}

func TestQueueEventsProcessorQueueAddedUpdatesPriorityGauge(t *testing.T) {
	priorities := []struct {
		priority  grader.QueuePriority
		gaugeName string
	}{
		{grader.QueuePriorityEphemeral, "grader_queue_ephemeral_length"},
		{grader.QueuePriorityLow, "grader_queue_low_length"},
		{grader.QueuePriorityNormal, "grader_queue_normal_length"},
		{grader.QueuePriorityHigh, "grader_queue_high_length"},
	}

	for _, tc := range priorities {
		t.Run(tc.gaugeName, func(t *testing.T) {
			metrics := &recordingMetrics{}
			feedQueueEvents(metrics, &grader.QueueEvent{
				Type:     grader.QueueEventTypeQueueAdded,
				Priority: tc.priority,
			})

			for _, p := range priorities {
				got := metrics.gaugeTotal(p.gaugeName)
				if p.gaugeName == tc.gaugeName {
					if got != 1 {
						t.Errorf("gauge %s = %f, want 1", p.gaugeName, got)
					}
				} else {
					if got != 0 {
						t.Errorf("gauge %s = %f, want 0", p.gaugeName, got)
					}
				}
			}

			if got := metrics.gaugeTotal("grader_queue_total_length"); got != 0 {
				t.Errorf("grader_queue_total_length = %f, want 0", got)
			}
		})
	}
}

func TestQueueEventsProcessorQueueRemovedUpdatesPriorityGauge(t *testing.T) {
	priorities := []struct {
		priority  grader.QueuePriority
		gaugeName string
	}{
		{grader.QueuePriorityEphemeral, "grader_queue_ephemeral_length"},
		{grader.QueuePriorityLow, "grader_queue_low_length"},
		{grader.QueuePriorityNormal, "grader_queue_normal_length"},
		{grader.QueuePriorityHigh, "grader_queue_high_length"},
	}

	for _, tc := range priorities {
		t.Run(tc.gaugeName, func(t *testing.T) {
			metrics := &recordingMetrics{}
			feedQueueEvents(metrics, &grader.QueueEvent{
				Type:     grader.QueueEventTypeQueueRemoved,
				Priority: tc.priority,
			})

			for _, p := range priorities {
				got := metrics.gaugeTotal(p.gaugeName)
				if p.gaugeName == tc.gaugeName {
					if got != -1 {
						t.Errorf("gauge %s = %f, want -1", p.gaugeName, got)
					}
				} else {
					if got != 0 {
						t.Errorf("gauge %s = %f, want 0", p.gaugeName, got)
					}
				}
			}

			if got := metrics.gaugeTotal("grader_queue_total_length"); got != 0 {
				t.Errorf("grader_queue_total_length = %f, want 0", got)
			}
		})
	}
}

func TestQueueEventsProcessorAddedAddedRemovedNetsToOne(t *testing.T) {
	metrics := &recordingMetrics{}
	feedQueueEvents(
		metrics,
		&grader.QueueEvent{
			Type:     grader.QueueEventTypeQueueAdded,
			Priority: grader.QueuePriorityNormal,
		},
		&grader.QueueEvent{
			Type:     grader.QueueEventTypeQueueAdded,
			Priority: grader.QueuePriorityNormal,
		},
		&grader.QueueEvent{
			Type:     grader.QueueEventTypeQueueRemoved,
			Priority: grader.QueuePriorityNormal,
		},
	)

	if got := metrics.gaugeTotal("grader_queue_normal_length"); got != 1 {
		t.Errorf("grader_queue_normal_length = %f, want 1", got)
	}
}
