package main

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/omegaup/quark/common"
	_ "github.com/mattn/go-sqlite3"
)

// setupTestDB creates an in-memory SQLite database for testing
func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %s", err)
	}

	// Create test tables
	_, err = db.Exec(`
		CREATE TABLE Runs (
			run_id INTEGER PRIMARY KEY,
			status VARCHAR(50) NOT NULL,
			submission_id INTEGER,
			verdict VARCHAR(10),
			runtime INTEGER,
			penalty INTEGER,
			memory INTEGER,
			score DECIMAL(5,2),
			contest_score DECIMAL(5,2),
			judged_by VARCHAR(255),
			version VARCHAR(64)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create Runs table: %s", err)
	}

	_, err = db.Exec(`
		CREATE TABLE Submissions (
			submission_id INTEGER PRIMARY KEY,
			guid VARCHAR(32) NOT NULL,
			identity_id INTEGER,
			problem_id INTEGER,
			problemset_id INTEGER,
			language VARCHAR(20),
			submit_delay INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create Submissions table: %s", err)
	}

	_, err = db.Exec(`
		CREATE TABLE Identities (
			identity_id INTEGER PRIMARY KEY,
			username VARCHAR(50) NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create Identities table: %s", err)
	}

	_, err = db.Exec(`
		CREATE TABLE Problems (
			problem_id INTEGER PRIMARY KEY,
			alias VARCHAR(100) NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create Problems table: %s", err)
	}

	return db
}

// insertTestData inserts sample data for testing
func insertTestData(t *testing.T, db *sql.DB, numPendingRuns int) {
	// Insert test identities
	_, err := db.Exec("INSERT INTO Identities (identity_id, username) VALUES (1, 'testuser')")
	if err != nil {
		t.Fatalf("Failed to insert test identity: %s", err)
	}

	// Insert test problems
	_, err = db.Exec("INSERT INTO Problems (problem_id, alias) VALUES (1, 'testproblem')")
	if err != nil {
		t.Fatalf("Failed to insert test problem: %s", err)
	}

	// Insert test submissions
	_, err = db.Exec(`
		INSERT INTO Submissions (submission_id, guid, identity_id, problem_id, language, submit_delay) 
		VALUES (1, 'abcdef1234567890abcdef1234567890', 1, 1, 'cpp', 0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test submission: %s", err)
	}

	// Insert test runs - some pending, some ready
	for i := 1; i <= numPendingRuns; i++ {
		status := "pending"
		if i%3 == 0 { // Every 3rd run is ready
			status = "ready"
		}
		_, err = db.Exec(`
			INSERT INTO Runs (run_id, status, submission_id, version) 
			VALUES (?, ?, 1, 'test_hash')
		`, i, status)
		if err != nil {
			t.Fatalf("Failed to insert test run %d: %s", i, err)
		}
	}
}

func newTestGraderContext(t *testing.T) *common.Context {
	// Use a minimal context for testing - avoid grader.Context dependencies
	ctx, err := common.NewContext(nil, "grader")
	if err != nil {
		// Create a basic context manually if NewContext fails
		ctx = &common.Context{}
	}
	return ctx
}

func TestGetPendingRunsBatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := newTestGraderContext(t)

	testCases := []struct {
		name           string
		totalRuns      int
		offset         int
		limit          int
		expectedCount  int
		expectError    bool
	}{
		{
			name:           "Small batch from start",
			totalRuns:      100,
			offset:         0,
			limit:          10,
			expectedCount:  10,
			expectError:    false,
		},
		{
			name:           "Batch with offset",
			totalRuns:      100,
			offset:         20,
			limit:          10,
			expectedCount:  10,
			expectError:    false,
		},
		{
			name:           "Large limit",
			totalRuns:      50,
			offset:         0,
			limit:          100,
			expectedCount:  34, // 50 total runs, but only ~67% are pending (50 - 50/3)
			expectError:    false,
		},
		{
			name:           "Beyond available data",
			totalRuns:      20,
			offset:         100,
			limit:          10,
			expectedCount:  0,
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean and setup data for each test case
			_, err := db.Exec("DELETE FROM Runs")
			if err != nil {
				t.Fatalf("Failed to clean runs table: %s", err)
			}

			insertTestData(t, db, tc.totalRuns)

			// Test the function
			runIds, err := getPendingRunsBatch(ctx, db, tc.offset, tc.limit)

			if tc.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error: %s", err)
			}

			if len(runIds) != tc.expectedCount {
				t.Errorf("Expected %d run IDs, got %d", tc.expectedCount, len(runIds))
			}

			// Validate that all returned IDs are for pending runs
			for _, runID := range runIds {
				var status string
				err = db.QueryRow("SELECT status FROM Runs WHERE run_id = ?", runID).Scan(&status)
				if err != nil {
					t.Errorf("Failed to query status for run %d: %s", runID, err)
				}
				if status == "ready" {
					t.Errorf("Found ready run %d in pending results", runID)
				}
			}

			// Validate ordering (should be ordered by run_id)
			for i := 1; i < len(runIds); i++ {
				if runIds[i] <= runIds[i-1] {
					t.Errorf("Run IDs not properly ordered: %d <= %d", runIds[i], runIds[i-1])
				}
			}
		})
	}
}

func TestProcessPendingRunsAsync(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := newTestGraderContext(t)
	
	// Mock a queue (simplified)
	queue := &mockQueue{processed: make([]int64, 0)}

	// Insert test data
	insertTestData(t, db, 150) // Will create ~100 pending runs

	// Test async processing
	done := make(chan bool)
	go func() {
		processPendingRunsAsync(ctx, db, queue)
		done <- true
	}()

	// Wait for completion with timeout
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Processing took too long")
	}

	// Validate results
	if len(queue.processed) == 0 {
		t.Error("No runs were processed")
	}

	t.Logf("Processed %d pending runs", len(queue.processed))

	// All processed runs should have been pending
	for _, runID := range queue.processed {
		var status string
		err := db.QueryRow("SELECT status FROM Runs WHERE run_id = ?", runID).Scan(&status)
		if err != nil {
			t.Errorf("Failed to query status for run %d: %s", runID, err)
		}
		if status == "ready" {
			t.Errorf("Processed a ready run %d, should only process pending", runID)
		}
	}
}

// mockQueue simulates a grader queue for testing
type mockQueue struct {
	processed []int64
}

func (mq *mockQueue) AddRun(runCtx *grader.RunContext) {
	mq.processed = append(mq.processed, runCtx.ID)
}

// Benchmark to measure performance improvements
func BenchmarkGetPendingRunsBatch(b *testing.B) {
	db := setupTestDB(&testing.T{})
	defer db.Close()
	
	ctx := newTestGraderContext(&testing.T{})
	insertTestData(&testing.T{}, db, 10000) // Large dataset

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := getPendingRunsBatch(ctx, db, 0, 100)
		if err != nil {
			b.Fatalf("Benchmark failed: %s", err)
		}
	}
}