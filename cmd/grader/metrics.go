package main

import (
	"fmt"
	"github.com/lhchavez/quark/grader"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"net/http"
	"time"
)

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
	}

	counters = map[string]prometheus.Counter{
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
			Help:       "The duration of a run in the queue",
			Name:       "queue_delay_seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
	}
)

func setupMetrics(ctx *grader.Context) {
	for _, gauge := range gauges {
		prometheus.MustRegister(gauge)
	}
	for _, counter := range counters {
		prometheus.MustRegister(counter)
	}
	for _, summary := range summaries {
		prometheus.MustRegister(summary)
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", prometheus.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", ctx.Config.Metrics.Port)
		ctx.Log.Error(
			"http listen and serve",
			"err", http.ListenAndServe(addr, metricsMux),
		)
	}()
	go func() {
		gaugesUpdate()
		time.Sleep(time.Duration(1) * time.Minute)
	}()
}

func gaugesUpdate() {
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
}

func gaugeAdd(name string, value float64) {
	gauges[name].Add(value)
}

func counterAdd(name string, value float64) {
	counters[name].Add(value)
}

func summaryObserve(name string, value float64) {
	summaries[name].Observe(value)
}
