package grader

import (
	"math/big"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omegaup/quark/common"
)

var (
	runID int64 = 1
)

func addRun(
	t *testing.T,
	ctx *Context,
	queue *Queue,
	priority QueuePriority,
) *RunInfo {
	AplusB, err := common.NewLiteralInputFactory(
		&common.LiteralInput{
			Cases: map[string]*common.LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
				"1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(1, 1)},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: common.ValidatorNameTokenNumeric,
			},
		},
		ctx.Config.Grader.RuntimePath,
		common.LiteralPersistGrader,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}
	inputRef, err := ctx.InputManager.Add(AplusB.Hash(), AplusB)
	if err != nil {
		t.Fatalf("Failed to get input back: %q", err)
	}

	originalLength := len(queue.runs[priority])

	artifactManager := NewArtifactManager(nil)
	runInfo := NewRunInfo()
	runInfo.ID = atomic.AddInt64(&runID, 1)
	runInfo.Priority = priority
	runInfo.Run.InputHash = inputRef.Input.Hash()
	runInfo.Run.Source = "print 3"
	runInfo.Artifacts = artifactManager.Grader(&ctx.Context, runInfo.ID)
	if err := queue.AddRun(&ctx.Context, runInfo, inputRef); err != nil {
		t.Fatalf("AddRunContext failed with %q", err)
	}

	if len(queue.runs[priority]) != originalLength+1 {
		t.Fatalf(
			"expected len(queue.runs[%d]) == %d, got %d",
			priority,
			originalLength+1,
			len(queue.runs[priority]),
		)
	}
	return runInfo
}

func TestMonitorSerializability(t *testing.T) {
	ctx, err := newGraderContext(t)
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
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	addRun(t, ctx, queue, QueuePriorityNormal)

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
	if _, _, ok := ctx.InflightMonitor.Get(runCtx.RunInfo.Run.AttemptID); !ok {
		t.Fatalf("Run %d not found in the inflight run monitor", runCtx.RunInfo.Run.AttemptID)
	}
	ctx.InflightMonitor.Remove(runCtx.RunInfo.Run.AttemptID)
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

func TestQueueRetry(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	listener := newListener()
	ctx.QueueManager.PostProcessor.AddListener(listener.c)

	closeNotifier := make(chan bool, 1)
	addRun(t, ctx, queue, QueuePriorityNormal)
	runCtx, _, _ := queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if !runCtx.Requeue(true) {
		t.Fatalf("unable to retry run")
	}
	if runCtx.Requeue(true) {
		t.Fatalf("run requeued even though it was marked as last attempt previously")
	}
	ctx.Close()

	<-listener.done
	if listener.processed != 1 {
		t.Fatalf("listener.processed == %d, want %d", listener.processed, 1)
	}
}

func TestQueuePriorities(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	closeNotifier := make(chan bool, 1)

	ephemeralPriority := addRun(t, ctx, queue, QueuePriorityEphemeral)
	lowPriority := addRun(t, ctx, queue, QueuePriorityLow)
	normalPriority := addRun(t, ctx, queue, QueuePriorityNormal)
	highPriority := addRun(t, ctx, queue, QueuePriorityHigh)

	var runCtx *RunContext
	runCtx, _, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if highPriority != runCtx.RunInfo {
		t.Fatalf("expected runCtx.RunInfo == %v, got %v", highPriority, runCtx.RunInfo)
	}
	runCtx, _, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if normalPriority != runCtx.RunInfo {
		t.Fatalf("expected runCtx == %v, got %v", normalPriority, runCtx.RunInfo)
	}
	runCtx, _, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if lowPriority != runCtx.RunInfo {
		t.Fatalf("expected runCtx == %v, got %v", lowPriority, runCtx.RunInfo)
	}
	runCtx, _, _ = queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if ephemeralPriority != runCtx.RunInfo {
		t.Fatalf("expected runCtx == %v, got %v", ephemeralPriority, runCtx.RunInfo)
	}

	queueInfo := ctx.QueueManager.GetQueueInfo()[DefaultQueueName]
	if queueInfo.Lengths[QueuePriorityEphemeral] != 0 {
		t.Errorf(
			"expected queueInfo.Lengths[%d] == %d, got %d",
			QueuePriorityEphemeral,
			0,
			queueInfo.Lengths[QueuePriorityEphemeral],
		)
	}
	if queueInfo.Lengths[QueuePriorityLow] != 0 {
		t.Errorf(
			"expected queueInfo.Lengths[%d] == %d, got %d",
			QueuePriorityLow,
			0,
			queueInfo.Lengths[QueuePriorityLow],
		)
	}
	if queueInfo.Lengths[QueuePriorityNormal] != 0 {
		t.Errorf(
			"expected queueInfo.Lengths[%d] == %d, got %d",
			QueuePriorityNormal,
			0,
			queueInfo.Lengths[QueuePriorityNormal],
		)
	}
	if queueInfo.Lengths[QueuePriorityHigh] != 0 {
		t.Errorf(
			"expected queueInfo.Lengths[%d] == %d, got %d",
			QueuePriorityHigh,
			0,
			queueInfo.Lengths[QueuePriorityHigh],
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
		for range l.c {
			l.processed++
		}
		close(l.done)
	}()
	return l
}

func TestPostProcessor(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

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

func waitForQueueEvent(t *testing.T, events <-chan *QueueEvent, typ QueueEventType) *QueueEvent {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("event channel closed while waiting for %v", typ)
			}
			if ev.Type == typ {
				return ev
			}
		case <-timeout:
			t.Fatalf("timed out waiting for event type %v", typ)
		}
	}
}

func TestEnqueueSetsQueueTime(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	runInfo := addRun(t, ctx, queue, QueuePriorityEphemeral)
	if runInfo.QueueTime.IsZero() {
		t.Fatal("QueueTime was not set when enqueueing an ephemeral run")
	}
}

func TestGetRunEmitsQueueRemovedUsingQueueTime(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	events := make(chan *QueueEvent, 16)
	ctx.QueueManager.AddEventListener(events)

	runInfo := addRun(t, ctx, queue, QueuePriorityNormal)
	runInfo.QueueTime = time.Now().Add(-250 * time.Millisecond)

	closeNotifier := make(chan bool, 1)
	runCtx, _, ok := queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if !ok {
		t.Fatal("expected a run from the queue")
	}

	removed := waitForQueueEvent(t, events, QueueEventTypeQueueRemoved)
	if removed.Delta < 200*time.Millisecond {
		t.Fatalf("QueueRemoved delta = %v, want at least 200ms of queue wait", removed.Delta)
	}

	ctx.InflightMonitor.Remove(runCtx.RunInfo.Run.AttemptID)

	timeout := time.After(200 * time.Millisecond)
	for {
		select {
		case ev := <-events:
			if ev.Type == QueueEventTypeQueueRemoved {
				t.Fatal("InflightMonitor.Remove emitted QueueRemoved; that event is dequeue wait time")
			}
		case <-timeout:
			return
		}
	}
}

func TestQueueAddedEmittedForEachPriority(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	events := make(chan *QueueEvent, 64)
	ctx.QueueManager.AddEventListener(events)

	priorities := []QueuePriority{
		QueuePriorityHigh,
		QueuePriorityNormal,
		QueuePriorityLow,
		QueuePriorityEphemeral,
	}

	for _, priority := range priorities {
		addRun(t, ctx, queue, priority)
		ev := waitForQueueEvent(t, events, QueueEventTypeQueueAdded)
		if ev.Priority != priority {
			t.Errorf("got QueueAdded priority %v, want %v", ev.Priority, priority)
		}
	}
}

func TestQueueLengthsMatchQueueInfo(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	events := make(chan *QueueEvent, 64)
	ctx.QueueManager.AddEventListener(events)

	lengths := make(map[QueuePriority]int)
	priorities := []QueuePriority{
		QueuePriorityHigh,
		QueuePriorityNormal,
		QueuePriorityLow,
		QueuePriorityEphemeral,
	}

	for _, priority := range priorities {
		addRun(t, ctx, queue, priority)
		ev := waitForQueueEvent(t, events, QueueEventTypeQueueAdded)
		lengths[ev.Priority]++
	}

	queueInfo := ctx.QueueManager.GetQueueInfo()[DefaultQueueName]
	for _, priority := range priorities {
		if lengths[priority] != 1 {
			t.Errorf("priority %v length from events = %d, want 1", priority, lengths[priority])
		}
		if queueInfo.Lengths[priority] != 1 {
			t.Errorf("priority %v length from queueInfo = %d, want 1", priority, queueInfo.Lengths[priority])
		}
		if lengths[priority] != queueInfo.Lengths[priority] {
			t.Errorf("priority %v events length %d != queueInfo length %d", priority, lengths[priority], queueInfo.Lengths[priority])
		}
	}

	closeNotifier := make(chan bool, 1)
	for i := 0; i < len(priorities); i++ {
		runCtx, _, ok := queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
		if !ok {
			t.Fatalf("failed to dequeue run %d", i)
		}
		defer ctx.InflightMonitor.Remove(runCtx.RunInfo.Run.AttemptID)
		ev := waitForQueueEvent(t, events, QueueEventTypeQueueRemoved)
		lengths[ev.Priority]--
	}

	queueInfo = ctx.QueueManager.GetQueueInfo()[DefaultQueueName]
	for _, priority := range priorities {
		if lengths[priority] != 0 {
			t.Errorf("priority %v length from events = %d after dequeue, want 0", priority, lengths[priority])
		}
		if queueInfo.Lengths[priority] != 0 {
			t.Errorf("priority %v length from queueInfo = %d after dequeue, want 0", priority, queueInfo.Lengths[priority])
		}
	}
}

func TestQueueRequeuePriorityEvents(t *testing.T) {
	ctx, err := newGraderContext(t)
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	queue, err := ctx.QueueManager.Get(DefaultQueueName)
	if err != nil {
		t.Fatalf("default queue not found")
	}

	events := make(chan *QueueEvent, 64)
	ctx.QueueManager.AddEventListener(events)

	lengths := make(map[QueuePriority]int)
	applyEvent := func(ev *QueueEvent) {
		if ev.Type == QueueEventTypeQueueAdded {
			lengths[ev.Priority]++
		} else if ev.Type == QueueEventTypeQueueRemoved {
			lengths[ev.Priority]--
		}
	}

	// Add a Normal run
	addRun(t, ctx, queue, QueuePriorityNormal)
	ev := waitForQueueEvent(t, events, QueueEventTypeQueueAdded)
	applyEvent(ev)

	// GetRun it
	closeNotifier := make(chan bool, 1)
	runCtx, _, ok := queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if !ok {
		t.Fatal("expected run from queue")
	}
	ev = waitForQueueEvent(t, events, QueueEventTypeQueueRemoved)
	applyEvent(ev)

	// Requeue(false)
	if !runCtx.Requeue(false) {
		t.Fatal("failed to requeue run")
	}
	ev = waitForQueueEvent(t, events, QueueEventTypeQueueAdded)
	applyEvent(ev)

	// Normal should be 0, High should be 1
	if lengths[QueuePriorityNormal] != 0 {
		t.Errorf("Normal queue length = %d, want 0", lengths[QueuePriorityNormal])
	}
	if lengths[QueuePriorityHigh] != 1 {
		t.Errorf("High queue length = %d, want 1", lengths[QueuePriorityHigh])
	}

	// GetRun again
	runCtx2, _, ok := queue.GetRun("test", ctx.InflightMonitor, closeNotifier)
	if !ok {
		t.Fatal("expected requeued run from queue")
	}
	defer ctx.InflightMonitor.Remove(runCtx2.RunInfo.Run.AttemptID)
	ev = waitForQueueEvent(t, events, QueueEventTypeQueueRemoved)
	applyEvent(ev)

	// Everything should be 0
	for p := QueuePriority(0); p < QueueCount; p++ {
		if lengths[p] != 0 {
			t.Errorf("priority %v length = %d, want 0", p, lengths[p])
		}
	}
}
