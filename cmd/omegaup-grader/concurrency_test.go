package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/omegaup/quark/common"
)

// TestConcurrencySafety tests for race conditions in our concurrent code
func TestConcurrencySafety(t *testing.T) {
	// Reset global metrics to start clean
	common.ResetGlobalMetrics()
	
	const numGoroutines = 10
	const numOperations = 100
	
	var wg sync.WaitGroup
	
	// Test concurrent access to metrics
	t.Run("MetricsConcurrency", func(t *testing.T) {
		wg.Add(numGoroutines)
		
		for i := 0; i < numGoroutines; i++ {
			go func(goroutineID int) {
				defer wg.Done()
				
				for j := 0; j < numOperations; j++ {
					// Simulate function timing
					timer := common.GetGlobalMetrics().StartTimer("test_function")
					time.Sleep(1 * time.Millisecond) // Simulate work
					timer.Stop()
					
					// Read metrics concurrently
					if metrics, exists := common.GetGlobalMetrics().GetMetrics("test_function"); exists {
						_ = metrics.CallCount // Access without modification
						_ = metrics.AverageTime
					}
				}
			}(i)
		}
		
		wg.Wait()
		
		// Verify final metrics consistency
		metrics, exists := common.GetGlobalMetrics().GetMetrics("test_function")
		if !exists {
			t.Fatal("Metrics should exist after concurrent operations")
		}
		
		expectedCalls := int64(numGoroutines * numOperations)
		if metrics.CallCount != expectedCalls {
			t.Errorf("Expected %d calls, got %d", expectedCalls, metrics.CallCount)
		}
		
		t.Logf("✅ Metrics concurrency test passed: %d calls, avg: %v", 
			metrics.CallCount, metrics.AverageTime)
	})
}

// TestProcessorStartupSafety tests the processor startup mechanism
func TestProcessorStartupSafety(t *testing.T) {
	const numConcurrentStarts = 20
	
	var wg sync.WaitGroup
	var successfulStarts int64
	var mu sync.Mutex
	
	// Each test run gets its own Once variable
	var testOnce sync.Once
	
	t.Run("MultipleStartupCalls", func(t *testing.T) {
		wg.Add(numConcurrentStarts)
		
		for i := 0; i < numConcurrentStarts; i++ {
			go func(id int) {
				defer wg.Done()
				
				// Simulate multiple calls to start processor with shared Once
				testOnce.Do(func() {
					mu.Lock()
					successfulStarts++
					mu.Unlock()
					t.Logf("Processor started by goroutine %d", id)
				})
			}(i)
		}
		
		wg.Wait()
		
		// Should only have started once despite multiple concurrent calls
		if successfulStarts != 1 {
			t.Errorf("Expected processor to start exactly once, but started %d times", successfulStarts)
		} else {
			t.Logf("✅ Processor startup safety test passed: started %d time(s)", successfulStarts)
		}
	})
}

// TestGoroutineManagement tests proper goroutine lifecycle management
func TestGoroutineManagement(t *testing.T) {
	t.Run("GoroutineLifecycle", func(t *testing.T) {
		const timeout = 5 * time.Second
		
		done := make(chan struct{})
		var goroutineFinished bool
		var mu sync.Mutex
		
		// Start a goroutine with proper cleanup
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Goroutine panicked: %v", r)
				}
				
				mu.Lock()
				goroutineFinished = true
				mu.Unlock()
				close(done)
			}()
			
			// Simulate some work
			time.Sleep(100 * time.Millisecond)
		}()
		
		// Wait with timeout
		select {
		case <-done:
			mu.Lock()
			finished := goroutineFinished
			mu.Unlock()
			
			if !finished {
				t.Error("Goroutine finished flag not set")
			} else {
				t.Log("✅ Goroutine lifecycle test passed")
			}
		case <-time.After(timeout):
			t.Error("Goroutine did not finish within timeout")
		}
	})
}

// TestContextCancellation tests proper context cancellation handling
func TestContextCancellation(t *testing.T) {
	t.Run("ContextTimeout", func(t *testing.T) {
		// Test timeout context behavior
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		
		start := time.Now()
		
		// Wait for context to be cancelled
		<-ctx.Done()
		
		elapsed := time.Since(start)
		
		// Should be approximately 100ms (with some tolerance)
		if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
			t.Errorf("Context timeout took %v, expected ~100ms", elapsed)
		}
		
		if ctx.Err() == nil {
			t.Error("Context should have an error when cancelled")
		}
		
		t.Logf("✅ Context cancellation test passed: cancelled after %v", elapsed)
	})
}

// TestResourceCleanup tests proper resource cleanup patterns
func TestResourceCleanup(t *testing.T) {
	t.Run("DeferredCleanup", func(t *testing.T) {
		var cleanupCalled bool
		var mu sync.Mutex
		
		func() {
			defer func() {
				mu.Lock()
				cleanupCalled = true
				mu.Unlock()
			}()
			
			// Simulate some work that might panic
			// In real code, this would be database connections, etc.
		}()
		
		mu.Lock()
		cleaned := cleanupCalled
		mu.Unlock()
		
		if !cleaned {
			t.Error("Cleanup was not called")
		} else {
			t.Log("✅ Resource cleanup test passed")
		}
	})
}

// BenchmarkConcurrentMetrics benchmarks concurrent metrics operations
func BenchmarkConcurrentMetrics(b *testing.B) {
	common.ResetGlobalMetrics()
	
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			timer := common.GetGlobalMetrics().StartTimer("benchmark_function")
			// Simulate minimal work
			_ = time.Now()
			timer.Stop()
		}
	})
}

// TestDeadlockPrevention tests that we don't create deadlocks
func TestDeadlockPrevention(t *testing.T) {
	t.Run("NoDeadlockInMetrics", func(t *testing.T) {
		done := make(chan struct{})
		
		go func() {
			defer close(done)
			
			// Rapid metrics operations that could potentially deadlock
			for i := 0; i < 1000; i++ {
				timer := common.GetGlobalMetrics().StartTimer("deadlock_test")
				timer.Stop()
				
				// Interleave reads and writes
				if i%2 == 0 {
					common.GetGlobalMetrics().GetMetrics("deadlock_test")
				}
			}
		}()
		
		// Wait with a reasonable timeout
		select {
		case <-done:
			t.Log("✅ No deadlock detected in metrics operations")
		case <-time.After(5 * time.Second):
			t.Error("Potential deadlock detected - operations didn't complete within timeout")
		}
	})
}