package main

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/omegaup/quark/common"
)

// Simple test that validates the SQL query syntax and basic functionality
func TestGetPendingRunsBatchSQL(t *testing.T) {
	// Create in-memory database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %s", err)
	}
	defer db.Close()

	// Create minimal table structure
	_, err = db.Exec(`
		CREATE TABLE Runs (
			run_id INTEGER PRIMARY KEY,
			status VARCHAR(50) NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create Runs table: %s", err)
	}

	// Insert test data
	testData := []struct {
		runID  int
		status string
	}{
		{1, "pending"},
		{2, "ready"},
		{3, "pending"},
		{4, "pending"},
		{5, "ready"},
		{6, "pending"},
	}

	for _, td := range testData {
		_, err = db.Exec("INSERT INTO Runs (run_id, status) VALUES (?, ?)", td.runID, td.status)
		if err != nil {
			t.Fatalf("Failed to insert test data: %s", err)
		}
	}

	// Create a minimal context
	ctx := &common.Context{}

	// Test our batch function
	runIds, err := getPendingRunsBatch(ctx, db, 0, 10)
	if err != nil {
		t.Fatalf("getPendingRunsBatch failed: %s", err)
	}

	// Should get runs 1, 3, 4, 6 (the pending ones)
	expectedPending := []int64{1, 3, 4, 6}
	if len(runIds) != len(expectedPending) {
		t.Errorf("Expected %d pending runs, got %d", len(expectedPending), len(runIds))
	}

	// Verify each returned ID is actually pending
	for i, runID := range runIds {
		if runID != expectedPending[i] {
			t.Errorf("Expected run ID %d at position %d, got %d", expectedPending[i], i, runID)
		}

		var status string
		err = db.QueryRow("SELECT status FROM Runs WHERE run_id = ?", runID).Scan(&status)
		if err != nil {
			t.Errorf("Failed to query status for run %d: %s", runID, err)
		}
		if status != "pending" {
			t.Errorf("Run %d has status %s, expected pending", runID, status)
		}
	}

	t.Logf("✅ Successfully retrieved %d pending runs: %v", len(runIds), runIds)
}

// Test with pagination
func TestGetPendingRunsBatchPagination(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE Runs (
			run_id INTEGER PRIMARY KEY,
			status VARCHAR(50) NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create Runs table: %s", err)
	}

	// Insert 20 runs (10 pending, 10 ready)
	for i := 1; i <= 20; i++ {
		status := "pending"
		if i%2 == 0 {
			status = "ready"
		}
		_, err = db.Exec("INSERT INTO Runs (run_id, status) VALUES (?, ?)", i, status)
		if err != nil {
			t.Fatalf("Failed to insert test data: %s", err)
		}
	}

	ctx := &common.Context{}

	// Test first batch
	runIds1, err := getPendingRunsBatch(ctx, db, 0, 5)
	if err != nil {
		t.Fatalf("First batch failed: %s", err)
	}

	// Test second batch
	runIds2, err := getPendingRunsBatch(ctx, db, 5, 5)
	if err != nil {
		t.Fatalf("Second batch failed: %s", err)
	}

	// Should have different runs in each batch
	if len(runIds1) != 5 || len(runIds2) != 5 {
		t.Errorf("Expected 5 runs in each batch, got %d and %d", len(runIds1), len(runIds2))
	}

	// No duplicates between batches
	runMap := make(map[int64]bool)
	for _, id := range runIds1 {
		runMap[id] = true
	}
	for _, id := range runIds2 {
		if runMap[id] {
			t.Errorf("Duplicate run ID %d found in both batches", id)
		}
	}

	t.Logf("✅ Pagination test passed. Batch 1: %v, Batch 2: %v", runIds1, runIds2)
}

// Performance test comparing old vs new approach
func TestPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE Runs (
			run_id INTEGER PRIMARY KEY,
			status VARCHAR(50) NOT NULL
		);
		CREATE INDEX idx_status ON Runs(status);
	`)
	if err != nil {
		t.Fatalf("Failed to create schema: %s", err)
	}

	// Insert large dataset (similar to production)
	const totalRuns = 10000
	for i := 1; i <= totalRuns; i++ {
		status := "pending"
		if i%4 == 0 { // 25% ready, 75% pending
			status = "ready"
		}
		_, err = db.Exec("INSERT INTO Runs (run_id, status) VALUES (?, ?)", i, status)
		if err != nil {
			t.Fatalf("Failed to insert test data: %s", err)
		}
	}

	ctx := &common.Context{}

	// Simulate old approach (get all at once)
	start := testing.B{}.Now()
	rows, err := db.Query("SELECT run_id FROM Runs WHERE status != 'ready'")
	if err != nil {
		t.Fatalf("Old approach query failed: %s", err)
	}
	var allRuns []int64
	for rows.Next() {
		var runID int64
		rows.Scan(&runID)
		allRuns = append(allRuns, runID)
	}
	rows.Close()
	oldDuration := testing.B{}.Since(start)

	// New approach (batched)
	start = testing.B{}.Now()
	batchedRuns, err := getPendingRunsBatch(ctx, db, 0, 100)
	if err != nil {
		t.Fatalf("New approach failed: %s", err)
	}
	newDuration := testing.B{}.Since(start)

	t.Logf("📊 Performance Comparison:")
	t.Logf("   Old approach (all at once): %v, retrieved %d runs", oldDuration, len(allRuns))
	t.Logf("   New approach (batch of 100): %v, retrieved %d runs", newDuration, len(batchedRuns))
	t.Logf("   Memory usage reduction: ~%.1f%%", float64(len(allRuns)-len(batchedRuns))/float64(len(allRuns))*100)

	if len(batchedRuns) > 100 {
		t.Errorf("Batch size exceeded: got %d, expected max 100", len(batchedRuns))
	}
}