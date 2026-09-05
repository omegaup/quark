package common

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// PerformanceMetrics tracks timing metrics for functions
type PerformanceMetrics struct {
	mu      sync.RWMutex
	metrics map[string]*FunctionMetrics
}

// FunctionMetrics holds timing statistics for a specific function
type FunctionMetrics struct {
	Name         string
	CallCount    int64
	TotalTime    time.Duration
	MinTime      time.Duration
	MaxTime      time.Duration
	AverageTime  time.Duration
	LastCallTime time.Time
}

// NewPerformanceMetrics creates a new performance metrics tracker
func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		metrics: make(map[string]*FunctionMetrics),
	}
}

// Timer represents a timing measurement in progress
type Timer struct {
	metrics      *PerformanceMetrics
	functionName string
	startTime    time.Time
}

// StartTimer begins timing a function call
func (pm *PerformanceMetrics) StartTimer(functionName string) *Timer {
	return &Timer{
		metrics:      pm,
		functionName: functionName,
		startTime:    time.Now(),
	}
}

// Stop completes the timing measurement and updates metrics
func (t *Timer) Stop() time.Duration {
	duration := time.Since(t.startTime)
	t.metrics.recordTiming(t.functionName, duration)
	return duration
}

// recordTiming records a timing measurement for a function
func (pm *PerformanceMetrics) recordTiming(functionName string, duration time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	metrics, exists := pm.metrics[functionName]
	if !exists {
		metrics = &FunctionMetrics{
			Name:    functionName,
			MinTime: duration,
			MaxTime: duration,
		}
		pm.metrics[functionName] = metrics
	}

	// Update metrics
	metrics.CallCount++
	metrics.TotalTime += duration
	metrics.LastCallTime = time.Now()

	// Update min/max
	if duration < metrics.MinTime {
		metrics.MinTime = duration
	}
	if duration > metrics.MaxTime {
		metrics.MaxTime = duration
	}

	// Calculate average
	metrics.AverageTime = metrics.TotalTime / time.Duration(metrics.CallCount)
}

// GetMetrics returns a copy of metrics for a specific function
func (pm *PerformanceMetrics) GetMetrics(functionName string) (*FunctionMetrics, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	metrics, exists := pm.metrics[functionName]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	copy := *metrics
	return &copy, true
}

// GetAllMetrics returns a copy of all metrics
func (pm *PerformanceMetrics) GetAllMetrics() map[string]*FunctionMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make(map[string]*FunctionMetrics)
	for name, metrics := range pm.metrics {
		copy := *metrics
		result[name] = &copy
	}
	return result
}

// Reset clears all metrics
func (pm *PerformanceMetrics) Reset() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.metrics = make(map[string]*FunctionMetrics)
}

// LogSlowCalls logs functions that exceed a threshold duration
func (pm *PerformanceMetrics) LogSlowCalls(threshold time.Duration, logger LogContext) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, metrics := range pm.metrics {
		if metrics.AverageTime > threshold || metrics.MaxTime > threshold {
			logger.Warn("Slow function detected",
				"function", metrics.Name,
				"call_count", metrics.CallCount,
				"average_time", metrics.AverageTime,
				"max_time", metrics.MaxTime,
				"min_time", metrics.MinTime,
				"total_time", metrics.TotalTime,
				"last_call", metrics.LastCallTime)
		}
	}
}

// Global metrics instance
var globalMetrics = NewPerformanceMetrics()

// TimeFunction is a helper function to time any function call
func TimeFunction(functionName string, fn func()) time.Duration {
	timer := globalMetrics.StartTimer(functionName)
	defer timer.Stop()
	fn()
	return timer.Stop()
}

// TimeFunctionWithReturn times a function that returns a value
func TimeFunctionWithReturn(functionName string, fn func() interface{}) (interface{}, time.Duration) {
	timer := globalMetrics.StartTimer(functionName)
	defer timer.Stop()
	result := fn()
	duration := timer.Stop()
	return result, duration
}

// TimeFunctionWithError times a function that returns an error
func TimeFunctionWithError(functionName string, fn func() error) (error, time.Duration) {
	timer := globalMetrics.StartTimer(functionName)
	defer timer.Stop()
	err := fn()
	duration := timer.Stop()
	return err, duration
}

// GetGlobalMetrics returns the global metrics instance
func GetGlobalMetrics() *PerformanceMetrics {
	return globalMetrics
}

// ResetGlobalMetrics resets all global metrics
func ResetGlobalMetrics() {
	globalMetrics.Reset()
}

// ========================================
// APDEX AND ADVANCED METRICS SYSTEM
// ========================================

// ApdexThresholds defines the satisfaction and tolerance thresholds for Apdex calculation
type ApdexThresholds struct {
	SatisfiedThreshold time.Duration // Time threshold for satisfied responses (T)
	ToleratedThreshold time.Duration // Time threshold for tolerated responses (4*T)
}

// DefaultApdexThresholds provides reasonable defaults for web applications
var DefaultApdexThresholds = ApdexThresholds{
	SatisfiedThreshold: 500 * time.Millisecond,  // 500ms for satisfaction
	ToleratedThreshold: 2000 * time.Millisecond, // 2s for tolerance (4*500ms)
}

// ApdexMetrics holds Apdex calculation data
type ApdexMetrics struct {
	mu                sync.RWMutex
	totalRequests     int64
	satisfiedCount    int64
	toleratedCount    int64
	frustratedCount   int64
	thresholds        ApdexThresholds
	lastCalculated    time.Time
	currentScore      float64
	operation         string
}

// ThroughputLatencyMetrics tracks correlation between throughput and latency
type ThroughputLatencyMetrics struct {
	mu             sync.RWMutex
	requestCount   int64
	totalLatency   int64 // Total latency in microseconds
	windowStart    time.Time
	windowDuration time.Duration
	samples        []LatencySample
	maxSamples     int
}

// LatencySample represents a latency measurement with timestamp
type LatencySample struct {
	Timestamp time.Time
	Latency   time.Duration
}

// PeakTrafficMetrics tracks traffic patterns by hour and day
type PeakTrafficMetrics struct {
	mu           sync.RWMutex
	hourlyPeaks  [24]int64        // Requests per hour (0-23)
	dailyPeaks   [7]int64         // Requests per day (0=Sunday)
	hourlyLatency [24][]time.Duration // Latency samples per hour
	dailyLatency [7][]time.Duration   // Latency samples per day
	lastUpdate   time.Time
}

// AlertConfig defines alerting configuration
type AlertConfig struct {
	ApdexThreshold    float64       // Alert when Apdex < this value
	LatencyThreshold  time.Duration // Alert when P95 latency > this value
	ThroughputDrop    float64       // Alert when throughput drops by this %
	AlertCooldown     time.Duration // Minimum time between alerts
	LastAlert         time.Time
}

// DefaultAlertConfig provides sensible alerting defaults
var DefaultAlertConfig = AlertConfig{
	ApdexThreshold:    0.5,                   // Alert when Apdex < 0.5
	LatencyThreshold:  3 * time.Second,       // Alert when P95 > 3s
	ThroughputDrop:    0.3,                   // Alert on 30% throughput drop
	AlertCooldown:     5 * time.Minute,       // 5 minutes between alerts
}

// MetricsCollector coordinates all metrics collection and alerting
type MetricsCollector struct {
	mu                   sync.RWMutex
	apdexMetrics         map[string]*ApdexMetrics
	throughputMetrics    map[string]*ThroughputLatencyMetrics
	peakMetrics          *PeakTrafficMetrics
	alertConfig          AlertConfig
	correlatedLogger     *CorrelatedLogger
	alertsTriggered      int64
	totalOperations      int64
}

// NewMetricsCollector creates a new metrics collection system
func NewMetricsCollector(traceCtx *TraceContext) *MetricsCollector {
	return &MetricsCollector{
		apdexMetrics:      make(map[string]*ApdexMetrics),
		throughputMetrics: make(map[string]*ThroughputLatencyMetrics),
		peakMetrics:       NewPeakTrafficMetrics(),
		alertConfig:       DefaultAlertConfig,
		correlatedLogger:  NewCorrelatedLogger(traceCtx),
	}
}

// RecordOperation records a completed operation with its duration
func (mc *MetricsCollector) RecordOperation(operation string, duration time.Duration, traceCtx *TraceContext) {
	atomic.AddInt64(&mc.totalOperations, 1)
	
	// Record Apdex metrics
	mc.recordApdex(operation, duration)
	
	// Record throughput/latency correlation
	mc.recordThroughputLatency(operation, duration)
	
	// Record peak traffic patterns
	mc.peakMetrics.RecordRequest(duration)
	
	// Check for alerts
	mc.checkAlerts(operation, duration, traceCtx)
}

// recordApdex updates Apdex metrics for an operation
func (mc *MetricsCollector) recordApdex(operation string, duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	apdex, exists := mc.apdexMetrics[operation]
	if !exists {
		apdex = &ApdexMetrics{
			thresholds:     DefaultApdexThresholds,
			operation:      operation,
			lastCalculated: time.Now(),
		}
		mc.apdexMetrics[operation] = apdex
	}
	
	apdex.mu.Lock()
	defer apdex.mu.Unlock()
	
	apdex.totalRequests++
	
	if duration <= apdex.thresholds.SatisfiedThreshold {
		apdex.satisfiedCount++
	} else if duration <= apdex.thresholds.ToleratedThreshold {
		apdex.toleratedCount++
	} else {
		apdex.frustratedCount++
	}
	
	// Recalculate Apdex score
	apdex.currentScore = apdex.calculateScore()
}

// calculateScore computes the current Apdex score
func (am *ApdexMetrics) calculateScore() float64 {
	if am.totalRequests == 0 {
		return 1.0 // Perfect score with no requests
	}
	
	// Apdex = (Satisfied Count + Tolerated Count/2) / Total Samples
	return float64(am.satisfiedCount+am.toleratedCount/2) / float64(am.totalRequests)
}

// recordThroughputLatency updates throughput/latency correlation metrics
func (mc *MetricsCollector) recordThroughputLatency(operation string, duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	tl, exists := mc.throughputMetrics[operation]
	if !exists {
		tl = &ThroughputLatencyMetrics{
			windowStart:    time.Now(),
			windowDuration: 1 * time.Minute, // 1-minute windows
			maxSamples:     1000,             // Keep last 1000 samples
		}
		mc.throughputMetrics[operation] = tl
	}
	
	tl.mu.Lock()
	defer tl.mu.Unlock()
	
	now := time.Now()
	
	// Reset window if needed
	if now.Sub(tl.windowStart) >= tl.windowDuration {
		tl.windowStart = now
		tl.requestCount = 0
		tl.totalLatency = 0
	}
	
	tl.requestCount++
	tl.totalLatency += duration.Microseconds()
	
	// Add sample to history
	sample := LatencySample{
		Timestamp: now,
		Latency:   duration,
	}
	
	tl.samples = append(tl.samples, sample)
	
	// Trim old samples
	if len(tl.samples) > tl.maxSamples {
		tl.samples = tl.samples[len(tl.samples)-tl.maxSamples:]
	}
}

// NewPeakTrafficMetrics creates a new peak traffic tracker
func NewPeakTrafficMetrics() *PeakTrafficMetrics {
	return &PeakTrafficMetrics{
		lastUpdate: time.Now(),
	}
}

// RecordRequest records a request for peak traffic analysis
func (ptm *PeakTrafficMetrics) RecordRequest(latency time.Duration) {
	ptm.mu.Lock()
	defer ptm.mu.Unlock()
	
	now := time.Now()
	hour := now.Hour()
	weekday := int(now.Weekday())
	
	// Update counters
	ptm.hourlyPeaks[hour]++
	ptm.dailyPeaks[weekday]++
	
	// Store latency samples (keep last 100 per hour/day)
	if ptm.hourlyLatency[hour] == nil {
		ptm.hourlyLatency[hour] = make([]time.Duration, 0, 100)
	}
	if ptm.dailyLatency[weekday] == nil {
		ptm.dailyLatency[weekday] = make([]time.Duration, 0, 100)
	}
	
	ptm.hourlyLatency[hour] = append(ptm.hourlyLatency[hour], latency)
	ptm.dailyLatency[weekday] = append(ptm.dailyLatency[weekday], latency)
	
	// Trim if too many samples
	if len(ptm.hourlyLatency[hour]) > 100 {
		ptm.hourlyLatency[hour] = ptm.hourlyLatency[hour][len(ptm.hourlyLatency[hour])-100:]
	}
	if len(ptm.dailyLatency[weekday]) > 100 {
		ptm.dailyLatency[weekday] = ptm.dailyLatency[weekday][len(ptm.dailyLatency[weekday])-100:]
	}
	
	ptm.lastUpdate = now
}

// checkAlerts evaluates alert conditions and triggers notifications
func (mc *MetricsCollector) checkAlerts(operation string, duration time.Duration, traceCtx *TraceContext) {
	now := time.Now()
	
	// Check cooldown period
	if now.Sub(mc.alertConfig.LastAlert) < mc.alertConfig.AlertCooldown {
		return
	}
	
	mc.mu.RLock()
	apdex := mc.apdexMetrics[operation]
	throughput := mc.throughputMetrics[operation]
	mc.mu.RUnlock()
	
	if apdex == nil {
		return
	}
	
	// Check Apdex threshold
	if apdex.currentScore < mc.alertConfig.ApdexThreshold {
		mc.triggerApdexAlert(operation, apdex.currentScore, traceCtx)
		return
	}
	
	// Check latency threshold
	if duration > mc.alertConfig.LatencyThreshold {
		mc.triggerLatencyAlert(operation, duration, traceCtx)
		return
	}
	
	// Check throughput drop (requires historical data)
	if throughput != nil {
		currentThroughput := mc.calculateThroughput(operation)
		if mc.detectThroughputDrop(operation, currentThroughput) {
			mc.triggerThroughputAlert(operation, currentThroughput, traceCtx)
			return
		}
	}
}

// triggerApdexAlert sends an Apdex-based alert
func (mc *MetricsCollector) triggerApdexAlert(operation string, score float64, traceCtx *TraceContext) {
	atomic.AddInt64(&mc.alertsTriggered, 1)
	mc.alertConfig.LastAlert = time.Now()
	
	alert := map[string]interface{}{
		"alert_type":     "apdex_degradation",
		"operation":      operation,
		"current_score":  score,
		"threshold":      mc.alertConfig.ApdexThreshold,
		"severity":       "HIGH",
		"timestamp":      time.Now().UTC(),
		"trace_id":       traceCtx.TraceID,
		"span_id":        traceCtx.SpanID,
	}
	
	alertJSON, _ := json.Marshal(alert)
	
	mc.correlatedLogger.Error(
		"ALERT: Apdex score degradation detected",
		fmt.Errorf("apdex_score=%.3f threshold=%.3f", score, mc.alertConfig.ApdexThreshold),
		"alert_data", string(alertJSON),
		"operation", operation,
	)
}

// triggerLatencyAlert sends a latency-based alert
func (mc *MetricsCollector) triggerLatencyAlert(operation string, latency time.Duration, traceCtx *TraceContext) {
	atomic.AddInt64(&mc.alertsTriggered, 1)
	mc.alertConfig.LastAlert = time.Now()
	
	alert := map[string]interface{}{
		"alert_type":     "high_latency",
		"operation":      operation,
		"current_latency": latency.String(),
		"threshold":      mc.alertConfig.LatencyThreshold.String(),
		"severity":       "MEDIUM",
		"timestamp":      time.Now().UTC(),
		"trace_id":       traceCtx.TraceID,
		"span_id":        traceCtx.SpanID,
	}
	
	alertJSON, _ := json.Marshal(alert)
	
	mc.correlatedLogger.Error(
		"ALERT: High latency detected",
		fmt.Errorf("latency=%v threshold=%v", latency, mc.alertConfig.LatencyThreshold),
		"alert_data", string(alertJSON),
		"operation", operation,
	)
}

// triggerThroughputAlert sends a throughput drop alert
func (mc *MetricsCollector) triggerThroughputAlert(operation string, currentThroughput float64, traceCtx *TraceContext) {
	atomic.AddInt64(&mc.alertsTriggered, 1)
	mc.alertConfig.LastAlert = time.Now()
	
	alert := map[string]interface{}{
		"alert_type":          "throughput_drop",
		"operation":           operation,
		"current_throughput":  currentThroughput,
		"drop_threshold":      mc.alertConfig.ThroughputDrop,
		"severity":            "HIGH",
		"timestamp":           time.Now().UTC(),
		"trace_id":            traceCtx.TraceID,
		"span_id":             traceCtx.SpanID,
	}
	
	alertJSON, _ := json.Marshal(alert)
	
	mc.correlatedLogger.Error(
		"ALERT: Throughput drop detected",
		fmt.Errorf("throughput=%.2f req/s drop_threshold=%.1f%%", currentThroughput, mc.alertConfig.ThroughputDrop*100),
		"alert_data", string(alertJSON),
		"operation", operation,
	)
}

// GetApdexScore returns the current Apdex score for an operation
func (mc *MetricsCollector) GetApdexScore(operation string) float64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	if apdex, exists := mc.apdexMetrics[operation]; exists {
		apdex.mu.RLock()
		defer apdex.mu.RUnlock()
		return apdex.currentScore
	}
	return 1.0 // Perfect score if no data
}

// calculateThroughput computes current throughput for an operation
func (mc *MetricsCollector) calculateThroughput(operation string) float64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	if tl, exists := mc.throughputMetrics[operation]; exists {
		tl.mu.RLock()
		defer tl.mu.RUnlock()
		
		elapsed := time.Since(tl.windowStart).Seconds()
		if elapsed > 0 {
			return float64(tl.requestCount) / elapsed
		}
	}
	return 0.0
}

// detectThroughputDrop checks if there's a significant throughput decrease
func (mc *MetricsCollector) detectThroughputDrop(operation string, currentThroughput float64) bool {
	// Simplified implementation - in production, compare with historical averages
	// For now, we'll consider any throughput < 1 req/s as potentially problematic
	return currentThroughput < 1.0 && mc.totalOperations > 100
}

// GetMetricsSnapshot returns a comprehensive metrics snapshot
func (mc *MetricsCollector) GetMetricsSnapshot() map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	snapshot := map[string]interface{}{
		"timestamp":         time.Now().UTC(),
		"total_operations":  atomic.LoadInt64(&mc.totalOperations),
		"alerts_triggered":  atomic.LoadInt64(&mc.alertsTriggered),
		"apdex_metrics":     make(map[string]interface{}),
		"throughput_metrics": make(map[string]interface{}),
		"peak_metrics":      mc.getPeakMetricsSnapshot(),
	}
	
	// Add Apdex metrics
	for operation, apdex := range mc.apdexMetrics {
		apdex.mu.RLock()
		snapshot["apdex_metrics"].(map[string]interface{})[operation] = map[string]interface{}{
			"score":           apdex.currentScore,
			"total_requests":  apdex.totalRequests,
			"satisfied":       apdex.satisfiedCount,
			"tolerated":       apdex.toleratedCount,
			"frustrated":      apdex.frustratedCount,
			"last_calculated": apdex.lastCalculated,
		}
		apdex.mu.RUnlock()
	}
	
	// Add throughput metrics
	for operation, tl := range mc.throughputMetrics {
		tl.mu.RLock()
		avgLatency := float64(0)
		if tl.requestCount > 0 {
			avgLatency = float64(tl.totalLatency) / float64(tl.requestCount)
		}
		
		throughput := mc.calculateThroughput(operation)
		
		snapshot["throughput_metrics"].(map[string]interface{})[operation] = map[string]interface{}{
			"requests_per_second": throughput,
			"average_latency_us":  avgLatency,
			"total_requests":      tl.requestCount,
			"window_start":        tl.windowStart,
		}
		tl.mu.RUnlock()
	}
	
	return snapshot
}

// getPeakMetricsSnapshot returns peak traffic metrics
func (mc *MetricsCollector) getPeakMetricsSnapshot() map[string]interface{} {
	mc.peakMetrics.mu.RLock()
	defer mc.peakMetrics.mu.RUnlock()
	
	// Calculate percentiles for current hour
	currentHour := time.Now().Hour()
	hourlyP95 := calculateP95(mc.peakMetrics.hourlyLatency[currentHour])
	
	return map[string]interface{}{
		"hourly_requests": mc.peakMetrics.hourlyPeaks,
		"daily_requests":  mc.peakMetrics.dailyPeaks,
		"current_hour":    currentHour,
		"hourly_p95_ms":   hourlyP95.Milliseconds(),
		"last_update":     mc.peakMetrics.lastUpdate,
	}
}

// calculateP95 computes the 95th percentile of latencies
func calculateP95(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	
	index := int(float64(len(sorted)) * 0.95)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	
	return sorted[index]
}

// Global advanced metrics instance
var globalAdvancedMetrics *MetricsCollector

// GetGlobalAdvancedMetrics returns the global advanced metrics instance
func GetGlobalAdvancedMetrics() *MetricsCollector {
	if globalAdvancedMetrics == nil {
		// Initialize with a basic trace context
		traceCtx := NewTraceContext("global_metrics")
		globalAdvancedMetrics = NewMetricsCollector(traceCtx)
	}
	return globalAdvancedMetrics
}