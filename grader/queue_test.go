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
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}
	ctx.InputManager.MarshalJSON()
	ctx.InflightMonitor.MarshalJSON()
	ctx.QueueManager.MarshalJSON()
}

func TestQueue(t *testing.T) {
	ctx, err := newGraderContext()
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get("default")
	if err != nil {
		t.Fatalf("default queue not found")
	}

	AplusB, err := common.NewLiteralInputFactory(
		&common.LiteralInput{
			Cases: map[string]common.LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3"},
				"1": {Input: "2 3", ExpectedOutput: "5"},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: "token-numeric",
			},
		},
		ctx.Config.Grader.RuntimePath,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}
	ctx.InputManager.Add(AplusB.Hash(), AplusB)
	input, err := ctx.InputManager.Get(AplusB.Hash())
	if err != nil {
		t.Fatalf("Failed to get input back: %q", err)
	}
	runCtx := NewEmptyRunContext(ctx)
	runCtx.Run.InputHash = AplusB.Hash()
	runCtx.Run.Source = "print 3"
	if err = AddRunContext(ctx, runCtx, input); err != nil {
		t.Fatalf("AddRunContext failed with %q", err)
	}
	queue.AddRun(runCtx)
	if len(queue.runs[1]) != 1 {
		t.Fatalf("len(queue.runs[1]) == %d, want %d", len(queue.runs[1]), 1)
	}

	closeNotifier := make(chan bool, 1)

	// Test timeout.
	originalConnectTimeout := ctx.InflightMonitor.connectTimeout
	ctx.InflightMonitor.connectTimeout = 0
	runCtx, timeout, _ := queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if len(queue.runs[1]) != 0 {
		t.Fatalf("len(queue.runs[1]) == %d, want %d", len(queue.runs[1]), 0)
	}
	if _, didTimeout := <-timeout; !didTimeout {
		t.Fatalf("expected timeout but did not happen")
	}
	ctx.InflightMonitor.connectTimeout = originalConnectTimeout

	// The run has already been requeued. This time it will be successful.
	if len(queue.runs[0]) != 1 {
		t.Fatalf("len(queue.runs[0]) == %d, want %d", len(queue.runs[0]), 1)
	}
	runCtx, timeout, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if len(queue.runs[0]) != 0 {
		t.Fatalf("len(queue.runs[0]) == %d, want %d", len(queue.runs[0]), 0)
	}
	if _, _, ok := ctx.InflightMonitor.Get(runCtx.Run.AttemptID); !ok {
		t.Fatalf("Run %d not found in the inflight run monitor", runCtx.Run.AttemptID)
	}
	ctx.InflightMonitor.Remove(runCtx.Run.AttemptID)
	if _, didTimeout := <-timeout; didTimeout {
		t.Fatalf("expected run completion, but did not happen")
	}

	// Test the closeNotifier.
	closeNotifier <- true
	if _, _, ok := queue.GetRun(
		"test",
		ctx.InflightMonitor,
		closeNotifier,
	); ok {
		t.Fatalf("Expected closeNotifier to cause no run to be available")
	}
}
