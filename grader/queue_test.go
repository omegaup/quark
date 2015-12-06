package grader

import (
	"github.com/lhchavez/quark/common"
	"os"
	"testing"
)

func TestMonitorSerializability(t *testing.T) {
	ctx, err := newGraderContext()
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	ctx.InflightMonitor.String()
	ctx.QueueManager.String()
}

func TestQueue(t *testing.T) {
	ctx, err := newGraderContext()
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	defer os.RemoveAll(ctx.Config.Grader.RuntimePath)

	inputManager := common.NewInputManager(&ctx.Context)
	queue, err := ctx.QueueManager.Get("default")
	if err != nil {
		t.Fatalf("default queue not found")
	}

	if _, err := queue.AddRun(ctx, 1, inputManager); err != nil {
		t.Fatalf("AddRun failed with %q", err)
	}
	if len(queue.runs[1]) != 1 {
		t.Fatalf("len(queue.runs[1]) == %d, want %d", len(queue.runs[1]), 1)
	}

	closeNotifier := make(chan bool, 1)
	timeout := make(chan bool)

	// Test timeout.
	originalConnectTimeout := ctx.InflightMonitor.connectTimeout
	ctx.InflightMonitor.connectTimeout = 0
	runCtx, _ := queue.GetRun("test", ctx.InflightMonitor, closeNotifier, timeout)
	if len(queue.runs[1]) != 0 {
		t.Fatalf("len(queue.runs[1]) == %d, want %d", len(queue.runs[1]), 0)
	}
	if <-timeout != true {
		t.Fatalf("expected timeout but did not happen")
	}
	ctx.InflightMonitor.connectTimeout = originalConnectTimeout

	// Try running it again, this time it will be successful.
	runCtx.Requeue()
	if len(queue.runs[0]) != 1 {
		t.Fatalf("len(queue.runs[0]) == %d, want %d", len(queue.runs[0]), 1)
	}
	runCtx, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier, timeout)
	if len(queue.runs[0]) != 0 {
		t.Fatalf("len(queue.runs[0]) == %d, want %d", len(queue.runs[0]), 0)
	}
	if _, ok := ctx.InflightMonitor.Get(runCtx.Run.ID); !ok {
		t.Fatalf("Run %d not found in the inflight run monitor", runCtx.Run.ID)
	}
	ctx.InflightMonitor.Remove(runCtx.Run.ID)
	if <-timeout != false {
		t.Fatalf("expected run completion, but did not happen")
	}

	// Test the closeNotifier.
	closeNotifier <- true
	if _, ok := queue.GetRun(
		"test",
		ctx.InflightMonitor,
		closeNotifier,
		timeout,
	); ok {
		t.Fatalf("Expected closeNotifier to cause no run to be available")
	}
}
