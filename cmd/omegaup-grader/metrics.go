package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/omegaup/quark/grader"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

type staticConfig struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels,omitempty"`
}

var (
	gauges = map[string]prometheus.Gauge{
		"cpu_load1": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "CPU load 1",
			Name:      "cpu_load1",
		}),
		"cpu_load5": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "CPU load 5",
			Name:      "cpu_load5",
		}),
		"cpu_load15": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "CPU load 15",
			Name:      "cpu_load15",
		}),
		"mem_total": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "Total amount of RAM",
			Name:      "mem_total",
		}),
		"mem_used": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "RAM used by programs",
			Name:      "mem_used",
		}),
		"disk_total": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "Total amount of RAM",
			Name:      "disk_total",
		}),
		"disk_used": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "RAM used by programs",
			Name:      "disk_used",
		}),
		"grader_queue_total_length": prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "The length of the queue",
			Name:      "queue_total_length",
		}),
		"grader_queue_ephemeral_length": prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "The length of the ephemeral queue",
			Name:      "queue_ephemeral_length",
		}),
		"grader_queue_low_length": prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "The length of the low-priority queue",
			Name:      "queue_low_length",
		}),
		"grader_queue_normal_length": prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "The length of the normal-priority queue",
			Name:      "queue_normal_length",
		}),
		"grader_queue_high_length": prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "The length of the high-priority queue",
			Name:      "queue_high_length",
		}),
	}

	gaugeVecs = map[string]*prometheus.GaugeVec{
		"grader_runner_up": prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "quark",
				Subsystem: "grader",
				Help:      "Graders seen in the last 5 minutes",
				Name:      "runner_up",
			},
			[]string{"runner"},
		),
	}

	counters = map[string]prometheus.Counter{
		"grader_ephemeral_runs_total": prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "Number of graded ephemeral runs",
			Name:      "ephemeral_runs_total",
		}),
		"grader_ci_jobs_total": prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "Number of CI jobs",
			Name:      "ci_jobs_total",
		}),
		"grader_runs_total": prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "Number of graded runs",
			Name:      "runs_total",
		}),
		"grader_runs_retry": prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "Number of runs that were retried",
			Name:      "runs_retry",
		}),
		"grader_runs_abandoned": prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "Number of runs that were abandoned",
			Name:      "runs_abandoned",
		}),
		"grader_runs_je": prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quark",
			Subsystem: "grader",
			Help:      "Number of runs that were JE",
			Name:      "runs_je",
		}),
	}

	summaries = map[string]prometheus.Summary{
		"grader_queue_delay_seconds": prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  "quark",
			Subsystem:  "grader",
			Help:       "The duration of a run in any queue",
			Name:       "queue_delay_seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
		"grader_queue_ephemeral_delay_seconds": prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  "quark",
			Subsystem:  "grader",
			Help:       "The duration of a run in the ephemeral queue",
			Name:       "queue_ephemeral_delay_seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
		"grader_queue_low_delay_seconds": prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  "quark",
			Subsystem:  "grader",
			Help:       "The duration of a run in the low-priority queue",
			Name:       "queue_low_delay_seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
		"grader_queue_normal_delay_seconds": prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  "quark",
			Subsystem:  "grader",
			Help:       "The duration of a run in the normal-priority queue",
			Name:       "queue_normal_delay_seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
		"grader_queue_high_delay_seconds": prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  "quark",
			Subsystem:  "grader",
			Help:       "The duration of a run in the high-priority queue",
			Name:       "queue_high_delay_seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
	}
)

type prometheusMetrics struct {
	sync.Mutex
	runnerLastSeen map[string]time.Time
}

func (p *prometheusMetrics) GaugeAdd(name string, value float64) {
	if gauge, ok := gauges[name]; ok {
		gauge.Add(value)
	}
}

func (p *prometheusMetrics) CounterAdd(name string, value float64) {
	if counter, ok := counters[name]; ok {
		counter.Add(value)
	}
}

func (p *prometheusMetrics) SummaryObserve(name string, value float64) {
	if summary, ok := summaries[name]; ok {
		summary.Observe(value)
	}
}

func (p *prometheusMetrics) RunnerObserve(name string) {
	p.Lock()
	p.runnerLastSeen[name] = time.Now()
	p.Unlock()
}

func setupMetrics(ctx *grader.Context) {
	for _, gauge := range gauges {
		prometheus.MustRegister(gauge)
	}
	for _, gaugeVec := range gaugeVecs {
		prometheus.MustRegister(gaugeVec)
	}
	for _, counter := range counters {
		prometheus.MustRegister(counter)
	}
	for _, summary := range summaries {
		prometheus.MustRegister(summary)
	}

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

	m := &prometheusMetrics{
		runnerLastSeen: make(map[string]time.Time),
	}
	ctx.Metrics = m

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/metrics/runners", m.runners)
	go func() {
		addr := fmt.Sprintf(":%d", ctx.Config.Metrics.Port)
		err := http.ListenAndServe(addr, metricsMux)
		if !errors.Is(err, http.ErrServerClosed) {
			ctx.Log.Error(
				"http listen and serve",
				map[string]any{
					"err": err,
				},
			)
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			m.gaugesUpdate()
		}
	}()
}

func (p *prometheusMetrics) gaugesUpdate() {
	if s, err := load.Avg(); err == nil {
		gauges["cpu_load1"].Set(s.Load1)
		gauges["cpu_load5"].Set(s.Load5)
		gauges["cpu_load15"].Set(s.Load15)
	}
	if s, err := mem.VirtualMemory(); err == nil {
		gauges["mem_total"].Set(float64(s.Total))
		gauges["mem_used"].Set(float64(s.Used))
	}
	if s, err := disk.Usage("/"); err == nil {
		gauges["disk_total"].Set(float64(s.Total))
		gauges["disk_used"].Set(float64(s.Used))
	}

	cutoffTime := time.Now().Add(-3 * time.Minute)
	shouldReset := false
	p.Lock()
	runners := make([]string, 0, len(p.runnerLastSeen))
	for name, t := range p.runnerLastSeen {
		if t.Before(cutoffTime) {
			shouldReset = true
			delete(p.runnerLastSeen, name)
			continue
		}
		runners = append(runners, name)
	}
	p.Unlock()

	v := gaugeVecs["grader_runner_up"]
	if shouldReset {
		v.Reset()
	}
	for _, name := range runners {
		v.WithLabelValues(name).Set(1.0)
	}
}

func (p *prometheusMetrics) runners(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Add("Content-Type", "application/json")

	var config staticConfig
	cutoffTime := time.Now().Add(-3 * time.Minute)
	p.Lock()
	for name, t := range p.runnerLastSeen {
		if t.Before(cutoffTime) {
			delete(p.runnerLastSeen, name)
			continue
		}
		config.Targets = append(config.Targets, name)
	}
	p.Unlock()

	json.NewEncoder(w).Encode([]staticConfig{config})
}
