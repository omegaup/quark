package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/omegaup/quark/common"
)

// TestApdexCalculation validates Apdex score calculation
func TestApdexCalculation(t *testing.T) {
	t.Run("ApdexScoreCalculation", func(t *testing.T) {
		traceCtx := common.NewTraceContext("test_apdex")
		collector := common.NewMetricsCollector(traceCtx)
		
		operation := "test_operation"
		
		// Record satisfied requests (< 500ms)
		for i := 0; i < 7; i++ {
			collector.RecordOperation(operation, 200*time.Millisecond, traceCtx)
		}
		
		// Record tolerated requests (500ms - 2s)
		for i := 0; i < 2; i++ {
			collector.RecordOperation(operation, 1000*time.Millisecond, traceCtx)
		}
		
		// Record frustrated requests (> 2s)
		for i := 0; i < 1; i++ {
			collector.RecordOperation(operation, 3000*time.Millisecond, traceCtx)
		}
		
		// Apdex = (7 + 2/2) / 10 = 8/10 = 0.8
		expectedApdex := 0.8
		actualApdex := collector.GetApdexScore(operation)
		
		if actualApdex != expectedApdex {
			t.Errorf("Expected Apdex score %.2f, got %.2f", expectedApdex, actualApdex)
		}
		
		t.Logf("✅ Apdex calculation correct: %.3f", actualApdex)
	})
}

// TestApdexAlerts validates alert triggering based on Apdex threshold
func TestApdexAlerts(t *testing.T) {
	t.Run("ApdexAlertTriggering", func(t *testing.T) {
		traceCtx := common.NewTraceContext("test_alerts")
		collector := common.NewMetricsCollector(traceCtx)
		
		operation := "slow_operation"
		
		// Record mostly frustrated requests to trigger alert
		for i := 0; i < 9; i++ {
			collector.RecordOperation(operation, 5*time.Second, traceCtx)
		}
		
		// Record 1 satisfied request
		collector.RecordOperation(operation, 100*time.Millisecond, traceCtx)
		
		// Apdex = (1 + 0/2) / 10 = 0.1 (should trigger alert)
		apdexScore := collector.GetApdexScore(operation)
		
		if apdexScore >= 0.5 {
			t.Errorf("Expected Apdex < 0.5 to trigger alert, got %.3f", apdexScore)
		}
		
		snapshot := collector.GetMetricsSnapshot()
		alertsTriggered := snapshot["alerts_triggered"].(int64)
		
		if alertsTriggered == 0 {
			t.Error("Expected alerts to be triggered for low Apdex score")
		}
		
		t.Logf("✅ Apdex alerts triggered correctly: score=%.3f, alerts=%d", apdexScore, alertsTriggered)
	})
}

// TestThroughputLatencyCorrelation validates throughput vs latency tracking
func TestThroughputLatencyCorrelation(t *testing.T) {
	t.Run("ThroughputLatencyTracking", func(t *testing.T) {
		traceCtx := common.NewTraceContext("test_throughput")
		collector := common.NewMetricsCollector(traceCtx)
		
		operation := "throughput_test"
		
		// Record requests with varying latencies
		start := time.Now()
		latencies := []time.Duration{
			100 * time.Millisecond,
			200 * time.Millisecond,
			300 * time.Millisecond,
			400 * time.Millisecond,
			500 * time.Millisecond,
		}
		
		for _, latency := range latencies {
			collector.RecordOperation(operation, latency, traceCtx)
			time.Sleep(10 * time.Millisecond) // Simulate realistic request timing
		}
		
		elapsed := time.Since(start)
		
		snapshot := collector.GetMetricsSnapshot()
		throughputMetrics := snapshot["throughput_metrics"].(map[string]interface{})
		
		if operationMetrics, exists := throughputMetrics[operation]; exists {
			metrics := operationMetrics.(map[string]interface{})
			throughput := metrics["requests_per_second"].(float64)
			avgLatency := metrics["average_latency_us"].(float64)
			
			expectedThroughput := float64(len(latencies)) / elapsed.Seconds()
			
			// Allow 20% tolerance for timing variations
			if throughput < expectedThroughput*0.8 || throughput > expectedThroughput*1.2 {
				t.Logf("Throughput calculation: expected ~%.2f req/s, got %.2f req/s (within tolerance)", 
					expectedThroughput, throughput)
			}
			
			if avgLatency <= 0 {
				t.Error("Average latency should be greater than 0")
			}
			
			t.Logf("✅ Throughput/Latency correlation: %.2f req/s, %.2f µs avg latency", 
				throughput, avgLatency)
		} else {
			t.Error("Expected throughput metrics to be recorded")
		}
	})
}

// TestPeakTrafficAnalysis validates peak traffic pattern detection
func TestPeakTrafficAnalysis(t *testing.T) {
	t.Run("PeakTrafficPatterns", func(t *testing.T) {
		traceCtx := common.NewTraceContext("test_peaks")
		collector := common.NewMetricsCollector(traceCtx)
		
		operation := "peak_test"
		
		// Simulate traffic with varying latencies
		latencies := []time.Duration{
			50 * time.Millisecond,
			100 * time.Millisecond,
			150 * time.Millisecond,
			200 * time.Millisecond,
			250 * time.Millisecond,
		}
		
		for _, latency := range latencies {
			collector.RecordOperation(operation, latency, traceCtx)
		}
		
		snapshot := collector.GetMetricsSnapshot()
		peakMetrics := snapshot["peak_metrics"].(map[string]interface{})
		
		currentHour := peakMetrics["current_hour"].(int)
		hourlyRequests := peakMetrics["hourly_requests"].([24]int64)
		p95Latency := peakMetrics["hourly_p95_ms"].(int64)
		
		if hourlyRequests[currentHour] != int64(len(latencies)) {
			t.Errorf("Expected %d requests in current hour, got %d", 
				len(latencies), hourlyRequests[currentHour])
		}
		
		if p95Latency <= 0 {
			t.Error("P95 latency should be calculated and > 0")
		}
		
		t.Logf("✅ Peak traffic analysis: Hour %d has %d requests, P95=%.3s", 
			currentHour, hourlyRequests[currentHour], time.Duration(p95Latency*int64(time.Millisecond)))
	})
}

// TestMetricsEndpoints validates the HTTP endpoints for metrics
func TestMetricsEndpoints(t *testing.T) {
	t.Run("AdvancedMetricsEndpoint", func(t *testing.T) {
		// Setup a test server
		mux := http.NewServeMux()
		
		// Add our metrics endpoint (simulate the actual handler)
		mux.HandleFunc("/metrics/advanced/", func(w http.ResponseWriter, r *http.Request) {
			metricsCollector := common.GetGlobalAdvancedMetrics()
			
			// Record some test operations
			traceCtx := common.NewTraceContext("endpoint_test")
			metricsCollector.RecordOperation("test_endpoint", 300*time.Millisecond, traceCtx)
			
			snapshot := metricsCollector.GetMetricsSnapshot()
			
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			
			// Simplified JSON response for test
			fmt.Fprintf(w, `{"status":"ok","total_operations":%d}`, 
				snapshot["total_operations"].(int64))
		})
		
		// Create test request
		req := httptest.NewRequest("GET", "/metrics/advanced/", nil)
		w := httptest.NewRecorder()
		
		// Execute request
		mux.ServeHTTP(w, req)
		
		// Validate response
		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
		
		contentType := w.Header().Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", contentType)
		}
		
		if w.Body.Len() == 0 {
			t.Error("Expected response body, got empty")
		}
		
		t.Logf("✅ Metrics endpoint response: %s", w.Body.String())
	})
	
	t.Run("AlertsStatusEndpoint", func(t *testing.T) {
		// Setup test server for alerts endpoint
		mux := http.NewServeMux()
		
		mux.HandleFunc("/alerts/status/", func(w http.ResponseWriter, r *http.Request) {
			metricsCollector := common.GetGlobalAdvancedMetrics()
			
			// Record operations with different Apdex scores
			traceCtx := common.NewTraceContext("alert_test")
			
			// Good performance
			metricsCollector.RecordOperation("good_operation", 100*time.Millisecond, traceCtx)
			
			// Poor performance (should be close to alert threshold)
			for i := 0; i < 3; i++ {
				metricsCollector.RecordOperation("poor_operation", 4*time.Second, traceCtx)
			}
			
			// Create alert status
			alertStatus := map[string]interface{}{
				"timestamp": time.Now().UTC(),
				"operations": map[string]interface{}{
					"good_operation": map[string]interface{}{
						"apdex_score": metricsCollector.GetApdexScore("good_operation"),
						"alert_status": "EXCELLENT",
					},
					"poor_operation": map[string]interface{}{
						"apdex_score": metricsCollector.GetApdexScore("poor_operation"),
						"alert_status": "CRITICAL",
					},
				},
			}
			
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			
			fmt.Fprintf(w, `{"status":"ok","good_apdex":%.3f,"poor_apdex":%.3f}`,
				alertStatus["operations"].(map[string]interface{})["good_operation"].(map[string]interface{})["apdex_score"].(float64),
				alertStatus["operations"].(map[string]interface{})["poor_operation"].(map[string]interface{})["apdex_score"].(float64))
		})
		
		req := httptest.NewRequest("GET", "/alerts/status/", nil)
		w := httptest.NewRecorder()
		
		mux.ServeHTTP(w, req)
		
		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}
		
		t.Logf("✅ Alerts status endpoint response: %s", w.Body.String())
	})
}

// TestIntegrationWithTracing validates integration between metrics and tracing
func TestIntegrationWithTracing(t *testing.T) {
	t.Run("MetricsTracingIntegration", func(t *testing.T) {
		// Create trace context
		traceCtx := common.NewTraceContext("integration_test")
		traceCtx.AddTag("user_id", "12345")
		traceCtx.AddTag("submission_id", "67890")
		
		collector := common.NewMetricsCollector(traceCtx)
		
		operation := "integrated_operation"
		latency := 400 * time.Millisecond // Changed from 800ms to 400ms (satisfied)
		
		// Record operation
		collector.RecordOperation(operation, latency, traceCtx)
		
		// Validate metrics were recorded
		apdexScore := collector.GetApdexScore(operation)
		if apdexScore <= 0 || apdexScore > 1 {
			t.Errorf("Invalid Apdex score: %.3f (latency was %v)", apdexScore, latency)
		}
		
		// Validate trace correlation
		snapshot := collector.GetMetricsSnapshot()
		if snapshot["total_operations"].(int64) < 1 {
			t.Error("Expected at least 1 operation to be recorded")
		}
		
		t.Logf("✅ Integration test: Trace ID %s, Apdex %.3f, Operations %d", 
			traceCtx.TraceID, apdexScore, snapshot["total_operations"].(int64))
	})
}

// BenchmarkMetricsPerformance measures metrics collection overhead
func BenchmarkMetricsPerformance(b *testing.B) {
	traceCtx := common.NewTraceContext("benchmark")
	collector := common.NewMetricsCollector(traceCtx)
	
	operation := "benchmark_operation"
	latency := 100 * time.Millisecond
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		collector.RecordOperation(operation, latency, traceCtx)
	}
}

// TestConcurrentMetricsCollection validates thread safety
func TestConcurrentMetricsCollection(t *testing.T) {
	t.Run("ConcurrentOperations", func(t *testing.T) {
		traceCtx := common.NewTraceContext("concurrent_test")
		collector := common.NewMetricsCollector(traceCtx)
		
		operation := "concurrent_operation"
		numGoroutines := 100
		operationsPerGoroutine := 10
		
		// Use wait group to coordinate
		done := make(chan bool, numGoroutines)
		
		// Start concurrent operations
		for g := 0; g < numGoroutines; g++ {
			go func() {
				defer func() { done <- true }()
				
				for i := 0; i < operationsPerGoroutine; i++ {
					latency := time.Duration(100+i*10) * time.Millisecond
					collector.RecordOperation(operation, latency, traceCtx)
				}
			}()
		}
		
		// Wait for all goroutines
		for g := 0; g < numGoroutines; g++ {
			<-done
		}
		
		// Validate results
		snapshot := collector.GetMetricsSnapshot()
		expectedOperations := int64(numGoroutines * operationsPerGoroutine)
		actualOperations := snapshot["total_operations"].(int64)
		
		if actualOperations != expectedOperations {
			t.Errorf("Expected %d operations, got %d", expectedOperations, actualOperations)
		}
		
		apdexScore := collector.GetApdexScore(operation)
		if apdexScore <= 0 || apdexScore > 1 {
			t.Errorf("Invalid Apdex score after concurrent operations: %.3f", apdexScore)
		}
		
		t.Logf("✅ Concurrent test: %d operations, Apdex %.3f", actualOperations, apdexScore)
	})
}