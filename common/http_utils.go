package common

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"time"
)

// HTTPRetryConfig contains configuration for HTTP retry logic
type HTTPRetryConfig struct {
	MaxRetries      int
	InitialWaitTime float32
	MaxWaitTime     float32
	WaitMultiplier  float32
	RequestTimeout  time.Duration // New: timeout per request
	TotalTimeout    time.Duration // New: total timeout for all retries
}

// DefaultHTTPRetryConfig returns a sensible default retry configuration
func DefaultHTTPRetryConfig() HTTPRetryConfig {
	return HTTPRetryConfig{
		MaxRetries:      5,
		InitialWaitTime: 1.0,
		MaxWaitTime:     64.0,
		WaitMultiplier:  2.0,
		RequestTimeout:  30 * time.Second, // 30s per request
		TotalTimeout:    5 * time.Minute,  // 5 minutes total
	}
}

// HTTPRequestFunc is a function type that performs an HTTP request
type HTTPRequestFunc func() (*http.Response, error)

// HTTPRequestFuncWithContext is a function type that performs an HTTP request with context
type HTTPRequestFuncWithContext func(ctx context.Context) (*http.Response, error)

// RetryHTTPRequest executes an HTTP request with retry logic and exponential backoff
func RetryHTTPRequest(
	ctx LogContext,
	requestFunc HTTPRequestFunc,
	config HTTPRetryConfig,
	operationName string,
) (*http.Response, error) {
	// Create context wrapper
	requestFuncWithCtx := func(reqCtx context.Context) (*http.Response, error) {
		return requestFunc()
	}
	
	return RetryHTTPRequestWithContext(ctx, requestFuncWithCtx, config, operationName)
}

// RetryHTTPRequestWithContext executes an HTTP request with context, retry logic and exponential backoff
func RetryHTTPRequestWithContext(
	ctx LogContext,
	requestFunc HTTPRequestFuncWithContext,
	config HTTPRetryConfig,
	operationName string,
) (*http.Response, error) {
	startTime := time.Now()
	
	// Create master context with total timeout
	masterCtx, cancel := context.WithTimeout(context.Background(), config.TotalTimeout)
	defer cancel()
	
	var sleepTime = config.InitialWaitTime
	var lastErr error

	for attempt := 0; attempt < config.MaxRetries; attempt++ {
		// Check if master context is expired
		select {
		case <-masterCtx.Done():
			return nil, fmt.Errorf("total timeout exceeded for %s after %v: %w", 
				operationName, time.Since(startTime), masterCtx.Err())
		default:
		}
		
		// Create context for this specific request attempt
		requestCtx, requestCancel := context.WithTimeout(masterCtx, config.RequestTimeout)
		
		attemptStart := time.Now()
		resp, err := requestFunc(requestCtx)
		attemptDuration := time.Since(attemptStart)
		
		requestCancel() // Always cancel to free resources

		if err != nil {
			lastErr = err
			// Check if it's a timeout or context cancellation error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				ctx.Info("Timeout during HTTP request, retrying",
					"operation", operationName,
					"attempt", attempt+1,
					"max_retries", config.MaxRetries,
					"duration", attemptDuration,
					"err", err)
			} else if err == context.DeadlineExceeded || err == context.Canceled {
				ctx.Info("Context timeout during HTTP request, retrying",
					"operation", operationName,
					"attempt", attempt+1,
					"max_retries", config.MaxRetries,
					"duration", attemptDuration,
					"err", err)
			} else {
				ctx.Warn("Error during HTTP request, retrying",
					"operation", operationName,
					"attempt", attempt+1,
					"max_retries", config.MaxRetries,
					"duration", attemptDuration,
					"err", err)
			}

			// Don't sleep on the last attempt
			if attempt < config.MaxRetries-1 {
				sleepDuration := time.Duration(rand.Float32()*sleepTime) * time.Second
				
				// Use context-aware sleep
				select {
				case <-time.After(sleepDuration):
					// Continue with next attempt
				case <-masterCtx.Done():
					return nil, fmt.Errorf("total timeout exceeded during retry delay for %s: %w", 
						operationName, masterCtx.Err())
				}
				
				if sleepTime < config.MaxWaitTime {
					sleepTime *= config.WaitMultiplier
				}
			}
			continue
		}

		// Check for HTTP error status codes (5xx are retryable, 4xx typically are not)
		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			resp.Body.Close()
			lastErr = &HTTPStatusError{
				StatusCode: resp.StatusCode,
				Operation:  operationName,
				Duration:   attemptDuration,
			}

			ctx.Warn("HTTP server error, retrying",
				"operation", operationName,
				"attempt", attempt+1,
				"max_retries", config.MaxRetries,
				"status_code", resp.StatusCode,
				"duration", attemptDuration)

			// Don't sleep on the last attempt
			if attempt < config.MaxRetries-1 {
				sleepDuration := time.Duration(rand.Float32()*sleepTime) * time.Second
				
				// Use context-aware sleep
				select {
				case <-time.After(sleepDuration):
					// Continue with next attempt
				case <-masterCtx.Done():
					return nil, fmt.Errorf("total timeout exceeded during retry delay for %s: %w", 
						operationName, masterCtx.Err())
				}
				
				if sleepTime < config.MaxWaitTime {
					sleepTime *= config.WaitMultiplier
				}
			}
			continue
		}

		// Success or non-retryable error (4xx)
		ctx.Info("HTTP request successful",
			"operation", operationName,
			"attempts", attempt+1,
			"total_duration", time.Since(startTime),
			"final_attempt_duration", attemptDuration,
			"status_code", resp.StatusCode)
		return resp, nil
	}

	totalDuration := time.Since(startTime)
	return nil, fmt.Errorf("%s failed after %d attempts in %v: %w", 
		operationName, config.MaxRetries, totalDuration, lastErr)
}

// HTTPStatusError represents an HTTP status code error
type HTTPStatusError struct {
	StatusCode int
	Operation  string
	Duration   time.Duration
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("%s failed with HTTP status %d (duration: %v)", 
		e.Operation, e.StatusCode, e.Duration)
}

// LogContext defines the interface for logging that RetryHTTPRequest expects
type LogContext interface {
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
}
