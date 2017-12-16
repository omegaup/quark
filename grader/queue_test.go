package grader

import (
	"github.com/lhchavez/quark/common"
	"os"
	"testing"
)

func addRun(
	t *testing.T,
	ctx *Context,
	queue *Queue,
	input common.Input,
	priority QueuePriority,
) *RunContext {
	runCtx := NewEmptyRunContext(ctx)
	runCtx.Priority = priority
	runCtx.Run.InputHash = input.Hash()
	runCtx.Run.Source = "print 3"
	if err := AddRunContext(ctx, runCtx, input); err != nil {
		t.Fatalf("AddRunContext failed with %q", err)
	}
	originalLength := len(queue.runs[priority])
	queue.AddRun(runCtx)
	if len(queue.runs[priority]) != originalLength+1 {
		t.Fatalf(
			"expected len(queue.runs[%d]) == %d, got %d",
			priority,
			originalLength+1,
			len(queue.runs[priority]),
		)
	}
	return runCtx
}

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
	addRun(t, ctx, queue, input, QueuePriorityNormal)

	closeNotifier := make(chan bool, 1)

	// Test timeout.
	originalConnectTimeout := ctx.InflightMonitor.connectTimeout
	ctx.InflightMonitor.connectTimeout = 0
	runCtx, timeout, _ := queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if len(queue.runs[QueuePriorityNormal]) != 0 {
		t.Fatalf(
			"expected len(queue.runs[1]) == %d, got %d",
			0,
			len(queue.runs[QueuePriorityNormal]),
		)
	}
	if _, didTimeout := <-timeout; !didTimeout {
		t.Fatalf("expected timeout but did not happen")
	}
	ctx.InflightMonitor.connectTimeout = originalConnectTimeout

	// The run has already been requeued. This time it will be successful.
	if len(queue.runs[QueuePriorityHigh]) != 1 {
		t.Fatalf(
			"expected len(queue.runs[0]) == %d, got %d",
			1,
			len(queue.runs[QueuePriorityHigh]),
		)
	}
	runCtx, timeout, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if len(queue.runs[QueuePriorityHigh]) != 0 {
		t.Fatalf(
			"expected len(queue.runs[0]) == %d, got %d",
			0,
			len(queue.runs[QueuePriorityHigh]),
		)
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

func TestQueuePriorities(t *testing.T) {
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

	closeNotifier := make(chan bool, 1)

	lowPriority := addRun(t, ctx, queue, input, QueuePriorityLow)
	normalPriority := addRun(t, ctx, queue, input, QueuePriorityNormal)
	highPriority := addRun(t, ctx, queue, input, QueuePriorityHigh)

	var runCtx *RunContext
	runCtx, _, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if runCtx != highPriority {
		t.Fatalf("expected runCtx == %v, got %v", highPriority, runCtx)
	}
	runCtx, _, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if runCtx != normalPriority {
		t.Fatalf("expected runCtx == %v, got %v", normalPriority, runCtx)
	}
	runCtx, _, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if runCtx != lowPriority {
		t.Fatalf("expected runCtx == %v, got %v", lowPriority, runCtx)
	}

	if len(queue.runs[QueuePriorityLow]) != 0 {
		t.Fatalf(
			"expected len(queue.Runs[%d]) == %d, got %d",
			QueuePriorityLow,
			0,
			len(queue.runs[QueuePriorityLow]),
		)
	}
	if len(queue.runs[QueuePriorityNormal]) != 0 {
		t.Fatalf(
			"expected len(queue.Runs[%d]) == %d, got %d",
			QueuePriorityNormal,
			0,
			len(queue.runs[QueuePriorityNormal]),
		)
	}
	if len(queue.runs[QueuePriorityHigh]) != 0 {
		t.Fatalf(
			"expected len(queue.Runs[%d]) == %d, got %d",
			QueuePriorityHigh,
			0,
			len(queue.runs[QueuePriorityHigh]),
		)
	}
}

type listener struct {
	c         chan *RunInfo
	done      chan struct{}
	processed int
}

func newListener() *listener {
	l := &listener{
		c:         make(chan *RunInfo, 0),
		done:      make(chan struct{}, 0),
		processed: 0,
	}
	go func() {
		for _ = range l.c {
			l.processed++
		}
		close(l.done)
	}()
	return l
}

func TestPostProcessor(t *testing.T) {
	ctx, err := newGraderContext()
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()

	pp := NewRunPostProcessor()
	go pp.run()
	listeners := make([]*listener, 10)

	for i := range listeners {
		listeners[i] = newListener()
		pp.AddListener(listeners[i].c)
	}

	numProcessed := 10
	for i := 0; i < numProcessed; i++ {
		pp.PostProcess(&RunInfo{})
	}

	pp.Close()

	for i := range listeners {
		select {
		case <-listeners[i].done:
		}
		if listeners[i].processed != numProcessed {
			t.Fatalf("listeners[%d].processed == %d, want %d", i, listeners[i].processed, numProcessed)
		}
	}
}
