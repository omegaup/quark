package main

import (
	"fmt"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"net/http"
)

var (
	counters = map[string]prometheus.Counter{
		"runner_validator_errors": prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "quark",
			Subsystem: "runner",
			Help:      "Number of validator errors",
			Name:      "validator_errors",
		}),
	}

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
			Help:      "Total disk space",
			Name:      "disk_total",
		}),
		"disk_used": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "os",
			Help:      "Used disk space",
			Name:      "disk_used",
		}),
		"io_time": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark I/O user time",
			Name:      "io_time",
		}),
		"io_wall_time": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark I/O wall time",
			Name:      "io_wall_time",
		}),
		"io_memory": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark I/O memory",
			Name:      "io_memory",
		}),
		"cpu_time": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark CPU user time",
			Name:      "cpu_time",
		}),
		"cpu_wall_time": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark CPU wall time",
			Name:      "cpu_wall_time",
		}),
		"cpu_memory": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark CPU memory",
			Name:      "cpu_memory",
		}),
		"memory_time": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark Memory user time",
			Name:      "memory_time",
		}),
		"memory_wall_time": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark Memory wall time",
			Name:      "memory_wall_time",
		}),
		"memory_memory": prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: "quark_benchmark",
			Help:      "Quark Benchmark Memory memory",
			Name:      "memory_memory",
		}),
	}
)

type prometheusMetrics struct {
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
}

func setupMetrics(ctx *common.Context) {
	for _, gauge := range gauges {
		prometheus.MustRegister(gauge)
	}
	for _, counter := range counters {
		prometheus.MustRegister(counter)
	}

	ctx.Metrics = &prometheusMetrics{}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", ctx.Config.Metrics.Port)
		ctx.Log.Error(
			"http listen and serve",
			"err", http.ListenAndServe(addr, metricsMux),
		)
	}()
}

func gaugesUpdate(results runner.BenchmarkResults) {
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

	if results != nil {
		gauges["io_time"].Set(results["IO"].Time)
		gauges["io_wall_time"].Set(results["IO"].WallTime)
		gauges["io_memory"].Set(float64(results["IO"].Memory))
		gauges["cpu_time"].Set(results["CPU"].Time)
		gauges["cpu_wall_time"].Set(results["CPU"].WallTime)
		gauges["cpu_memory"].Set(float64(results["CPU"].Memory))
		gauges["memory_time"].Set(results["Memory"].Time)
		gauges["memory_wall_time"].Set(results["Memory"].WallTime)
		gauges["memory_memory"].Set(float64(results["Memory"].Memory))
	}
}
