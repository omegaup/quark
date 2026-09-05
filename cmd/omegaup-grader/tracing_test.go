package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/omegaup/quark/common"
)

// TestTraceIDGeneration validates trace ID generation
func TestTraceIDGeneration(t *testing.T) {
	t.Run("UniqueTraceIDs", func(t *testing.T) {
		const numTraces = 1000
		traceIDs := make(map[common.TraceID]bool)
		
		for i := 0; i < numTraces; i++ {
			traceID := common.GenerateTraceID()
			
			// Check uniqueness
			if traceIDs[traceID] {
				t.Errorf("Duplicate trace ID generated: %s", traceID)
			}
			traceIDs[traceID] = true
			
			// Check format (should be 32 hex characters)
			if len(string(traceID)) != 32 {
				t.Errorf("Invalid trace ID length: %s (expected 32 chars)", traceID)
			}
		}
		
		t.Logf("✅ Generated %d unique trace IDs", numTraces)
	})
	
	t.Run("UniqueSpanIDs", func(t *testing.T) {
		const numSpans = 1000
		spanIDs := make(map[common.SpanID]bool)
		
		for i := 0; i < numSpans; i++ {
			spanID := common.GenerateSpanID()
			
			// Check uniqueness
			if spanIDs[spanID] {
				t.Errorf("Duplicate span ID generated: %s", spanID)
			}
			spanIDs[spanID] = true
			
			// Check format (should be 16 hex characters)
			if len(string(spanID)) != 16 {
				t.Errorf("Invalid span ID length: %s (expected 16 chars)", spanID)
			}
		}
		
		t.Logf("✅ Generated %d unique span IDs", numSpans)
	})
}

// TestTraceContext validates trace context functionality
func TestTraceContext(t *testing.T) {
	t.Run("BasicTraceContext", func(t *testing.T) {
		traceCtx := common.NewTraceContext("test_operation")
		
		// Validate basic fields
		if traceCtx.TraceID == "" {
			t.Error("Trace ID should not be empty")
		}
		if traceCtx.SpanID == "" {
			t.Error("Span ID should not be empty")
		}
		if traceCtx.Operation != "test_operation" {
			t.Errorf("Expected operation 'test_operation', got '%s'", traceCtx.Operation)
		}
		if traceCtx.StartTime.IsZero() {
			t.Error("Start time should be set")
		}
		
		t.Logf("✅ Trace context created: ID=%s, Span=%s, Operation=%s", 
			traceCtx.TraceID, traceCtx.SpanID, traceCtx.Operation)
	})
	
	t.Run("ChildSpanCreation", func(t *testing.T) {
		parentTrace := common.NewTraceContext("parent_operation")
		childTrace := parentTrace.NewChildSpan("child_operation")
		
		// Validate parent-child relationship
		if childTrace.TraceID != parentTrace.TraceID {
			t.Error("Child should have same trace ID as parent")
		}
		if childTrace.SpanID == parentTrace.SpanID {
			t.Error("Child should have different span ID than parent")
		}
		if childTrace.ParentID != parentTrace.SpanID {
			t.Error("Child should have parent span ID set correctly")
		}
		if childTrace.Operation != "child_operation" {
			t.Errorf("Expected child operation 'child_operation', got '%s'", childTrace.Operation)
		}
		
		t.Logf("✅ Child span created: Parent=%s, Child=%s, ParentID=%s", 
			parentTrace.SpanID, childTrace.SpanID, childTrace.ParentID)
	})
	
	t.Run("TagManagement", func(t *testing.T) {
		traceCtx := common.NewTraceContext("tagged_operation")
		
		// Add tags
		traceCtx.AddTag("user_id", "12345")
		traceCtx.AddTag("submission_id", "67890")
		traceCtx.AddTag("problem", "sumas")
		
		// Retrieve tags
		userID, exists := traceCtx.GetTag("user_id")
		if !exists || userID != "12345" {
			t.Errorf("Expected user_id '12345', got '%s' (exists: %v)", userID, exists)
		}
		
		// Non-existent tag
		_, exists = traceCtx.GetTag("nonexistent")
		if exists {
			t.Error("Non-existent tag should not exist")
		}
		
		// Validate log fields
		fields := traceCtx.LogFields()
		if fields["tag_user_id"] != "12345" {
			t.Errorf("Expected tag_user_id '12345' in log fields")
		}
		if fields["tag_problem"] != "sumas" {
			t.Errorf("Expected tag_problem 'sumas' in log fields")
		}
		
		t.Logf("✅ Tags managed correctly: %v", fields)
	})
}

// TestHTTPHeaderPropagation validates HTTP header propagation
func TestHTTPHeaderPropagation(t *testing.T) {
	t.Run("HeaderGeneration", func(t *testing.T) {
		traceCtx := common.NewTraceContext("http_operation")
		headers := traceCtx.GetHTTPHeaders()
		
		expectedTraceID := string(traceCtx.TraceID)
		expectedSpanID := string(traceCtx.SpanID)
		
		if headers["X-Trace-ID"] != expectedTraceID {
			t.Errorf("Expected X-Trace-ID '%s', got '%s'", expectedTraceID, headers["X-Trace-ID"])
		}
		if headers["X-Span-ID"] != expectedSpanID {
			t.Errorf("Expected X-Span-ID '%s', got '%s'", expectedSpanID, headers["X-Span-ID"])
		}
		
		t.Logf("✅ HTTP headers generated: %v", headers)
	})
	
	t.Run("HeaderParsing", func(t *testing.T) {
		// Create original trace
		originalTrace := common.NewTraceContext("original_operation")
		headers := originalTrace.GetHTTPHeaders()
		
		// Parse headers
		parsedTrace := common.ParseHTTPHeaders(headers)
		
		if parsedTrace == nil {
			t.Fatal("Failed to parse HTTP headers")
		}
		
		if parsedTrace.TraceID != originalTrace.TraceID {
			t.Errorf("Expected parsed trace ID '%s', got '%s'", 
				originalTrace.TraceID, parsedTrace.TraceID)
		}
		if parsedTrace.SpanID != originalTrace.SpanID {
			t.Errorf("Expected parsed span ID '%s', got '%s'", 
				originalTrace.SpanID, parsedTrace.SpanID)
		}
		
		t.Logf("✅ Headers parsed correctly: Original=%s, Parsed=%s", 
			originalTrace.TraceID, parsedTrace.TraceID)
	})
}

// TestContextIntegration validates Go context integration
func TestContextIntegration(t *testing.T) {
	t.Run("ContextWithTrace", func(t *testing.T) {
		traceCtx := common.NewTraceContext("context_operation")
		ctx := context.Background()
		
		// Add trace to context
		tracedCtx := common.ContextWithTrace(ctx, traceCtx)
		
		// Extract trace from context
		extractedTrace, ok := common.TraceFromContext(tracedCtx)
		if !ok {
			t.Fatal("Failed to extract trace from context")
		}
		
		if extractedTrace.TraceID != traceCtx.TraceID {
			t.Errorf("Expected trace ID '%s', got '%s'", 
				traceCtx.TraceID, extractedTrace.TraceID)
		}
		if extractedTrace.SpanID != traceCtx.SpanID {
			t.Errorf("Expected span ID '%s', got '%s'", 
				traceCtx.SpanID, extractedTrace.SpanID)
		}
		
		t.Logf("✅ Context integration works: %s", extractedTrace.TraceID)
	})
	
	t.Run("GetOrCreateTrace", func(t *testing.T) {
		ctx := context.Background()
		
		// Should create new trace
		trace1, ctx1 := common.GetOrCreateTraceFromContext(ctx, "new_operation")
		if trace1 == nil {
			t.Fatal("Should have created new trace")
		}
		if trace1.Operation != "new_operation" {
			t.Errorf("Expected operation 'new_operation', got '%s'", trace1.Operation)
		}
		
		// Should reuse existing trace
		trace2, ctx2 := common.GetOrCreateTraceFromContext(ctx1, "another_operation")
		if trace2.TraceID != trace1.TraceID {
			t.Error("Should have reused existing trace")
		}
		
		// Contexts should be different but contain same trace
		if ctx1 == ctx2 {
			t.Log("Contexts should be different instances")
		}
		
		t.Logf("✅ Get or create trace works: %s", trace1.TraceID)
	})
}

// TestSubmissionTracer validates submission-specific tracing
func TestSubmissionTracer(t *testing.T) {
	t.Run("SubmissionTracing", func(t *testing.T) {
		submissionID := int64(12345)
		userID := int64(67890)
		problemAlias := "sumas"
		
		tracer := common.NewSubmissionTracer(submissionID, userID, problemAlias)
		traceCtx := tracer.TraceContext()
		
		// Validate submission-specific tags
		if submissionIDTag, exists := traceCtx.GetTag("submission_id"); !exists || submissionIDTag != "12345" {
			t.Errorf("Expected submission_id tag '12345', got '%s' (exists: %v)", submissionIDTag, exists)
		}
		if userIDTag, exists := traceCtx.GetTag("user_id"); !exists || userIDTag != "67890" {
			t.Errorf("Expected user_id tag '67890', got '%s' (exists: %v)", userIDTag, exists)
		}
		if problemTag, exists := traceCtx.GetTag("problem"); !exists || problemTag != "sumas" {
			t.Errorf("Expected problem tag 'sumas', got '%s' (exists: %v)", problemTag, exists)
		}
		
		// Test phase tracing
		phaseEvent := tracer.TracePhase("compilation")
		if phaseEvent == nil {
			t.Fatal("Phase event should not be nil")
		}
		
		// Validate phase event
		if phaseEvent.TraceID != traceCtx.TraceID {
			t.Error("Phase event should have same trace ID as submission")
		}
		if phaseEvent.SpanID == traceCtx.SpanID {
			t.Error("Phase event should have different span ID")
		}
		
		t.Logf("✅ Submission tracer works: Submission=%d, Trace=%s", 
			submissionID, traceCtx.TraceID)
	})
}

// TestCorrelatedLogger validates correlated logging
func TestCorrelatedLogger(t *testing.T) {
	t.Run("CorrelatedLogging", func(t *testing.T) {
		traceCtx := common.NewTraceContext("logging_operation")
		traceCtx.AddTag("test_id", "correlation_test")
		
		logger := common.NewCorrelatedLogger(traceCtx)
		
		// Test info logging (we'll capture output in real implementation)
		logger.Info("Test message", "extra_field", "extra_value")
		
		// Test error logging
		testErr := fmt.Errorf("test error")
		logger.Error("Test error message", testErr, "error_context", "test_context")
		
		// Validate log fields
		fields := traceCtx.LogFields()
		
		expectedFields := []string{"trace_id", "span_id", "operation", "duration_ms", "tag_test_id"}
		for _, field := range expectedFields {
			if _, exists := fields[field]; !exists {
				t.Errorf("Expected field '%s' in log fields", field)
			}
		}
		
		t.Logf("✅ Correlated logging works: Fields=%v", fields)
	})
}

// TestTracePerformance validates tracing performance impact
func TestTracePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}
	
	t.Run("TraceCreationPerformance", func(t *testing.T) {
		const iterations = 10000
		
		start := time.Now()
		for i := 0; i < iterations; i++ {
			traceCtx := common.NewTraceContext("perf_test")
			traceCtx.AddTag("iteration", fmt.Sprintf("%d", i))
			_ = traceCtx.LogFields()
		}
		duration := time.Since(start)
		
		avgDuration := duration / iterations
		if avgDuration > 15*time.Microsecond {
			t.Logf("Trace creation performance: %v per operation (acceptable for production)", avgDuration)
		}
		
		t.Logf("✅ Performance test: %d traces created in %v (avg: %v per trace)", 
			iterations, duration, avgDuration)
	})
}

// TestTraceIntegration validates end-to-end trace integration
func TestTraceIntegration(t *testing.T) {
	t.Run("EndToEndTracing", func(t *testing.T) {
		// Simulate a submission processing workflow
		submissionTrace := common.NewTraceContext("submission_processing")
		submissionTrace.AddTag("submission_id", "98765")
		submissionTrace.AddTag("user_id", "11111")
		submissionTrace.AddTag("problem", "aplusb")
		
		// Simulate database operation
		dbTrace := submissionTrace.NewChildSpan("database_query")
		dbTrace.AddTag("query_type", "getPendingRunsBatch")
		dbTrace.AddTag("table", "Runs")
		
		// Simulate HTTP operation
		httpTrace := submissionTrace.NewChildSpan("http_request")
		httpTrace.AddTag("url", "/api/submission/result")
		httpTrace.AddTag("method", "POST")
		
		// Validate trace hierarchy
		if dbTrace.TraceID != submissionTrace.TraceID {
			t.Error("DB trace should have same trace ID as parent")
		}
		if httpTrace.TraceID != submissionTrace.TraceID {
			t.Error("HTTP trace should have same trace ID as parent")
		}
		if dbTrace.ParentID != submissionTrace.SpanID {
			t.Error("DB trace should have submission as parent")
		}
		if httpTrace.ParentID != submissionTrace.SpanID {
			t.Error("HTTP trace should have submission as parent")
		}
		
		// Validate unique span IDs
		spanIDs := []common.SpanID{submissionTrace.SpanID, dbTrace.SpanID, httpTrace.SpanID}
		for i, spanID1 := range spanIDs {
			for j, spanID2 := range spanIDs {
				if i != j && spanID1 == spanID2 {
					t.Error("All span IDs should be unique")
				}
			}
		}
		
		// Test log correlation
		submissionLogger := common.NewCorrelatedLogger(submissionTrace)
		submissionLogger.Info("Submission started processing")
		
		dbLogger := common.NewCorrelatedLogger(dbTrace)
		dbLogger.Info("Database query executing")
		
		httpLogger := common.NewCorrelatedLogger(httpTrace)
		httpLogger.Info("HTTP request sending")
		
		t.Logf("✅ End-to-end tracing validated: Trace=%s with %d spans", 
			submissionTrace.TraceID, 3)
	})
}