package grader

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/go-base/v3/tracing"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"github.com/pkg/errors"
)

// QueuePriority represents the relative priority of a queue with respect with
// other queues. All the runs in a higher priority queue will be run before
// those in a lower priority queue.
type QueuePriority int

// QueueEventType represents the type of event that just occurred.
type QueueEventType int

const (
	// QueuePriorityHigh represents a QueuePriority with high priority.
	QueuePriorityHigh = QueuePriority(0)
	// QueuePriorityNormal represents a QueuePriority with normal priority.
	QueuePriorityNormal = QueuePriority(1)
	// QueuePriorityLow represents a QueuePriority with low priority.
	QueuePriorityLow = QueuePriority(2)
	// QueuePriorityEphemeral represents a QueuePriority with the lowest
	// priority. This also does not persist the results in the filesystem.
	QueuePriorityEphemeral = QueuePriority(3)

	// QueueCount represents the total number of queues in the grader.
	QueueCount = 4

	// DefaultQueueName is the default queue name.
	DefaultQueueName = "default"

	// QueueEventTypeManagerAdded represents when a run is added to the QueueManager.
	QueueEventTypeManagerAdded QueueEventType = iota

	// QueueEventTypeManagerRemoved represents when a run is removed from the QueueManager.
	QueueEventTypeManagerRemoved

	// QueueEventTypeQueueAdded represents when a run is added to a Queue.
	QueueEventTypeQueueAdded

	// QueueEventTypeQueueRemoved represents when a run is removed from a Qeuue.
	QueueEventTypeQueueRemoved

	// QueueEventTypeRetried represents when a run is retried.
	QueueEventTypeRetried

	// QueueEventTypeAbandoned represents when a run is abandoned due to too many retries.
	QueueEventTypeAbandoned
)

// A EphemeralRunRequest represents a client's request to run some code.
type EphemeralRunRequest struct {
	Source   string               `json:"source"`
	Language string               `json:"language"`
	Input    *common.LiteralInput `json:"input"`
}

// EphemeralRunManager handles a queue of recently-submitted ephemeral runs.
// This has a fixed maximum size with a last-in, first-out eviction policy.
type EphemeralRunManager struct {
	sync.Mutex
	evictList *list.List
	ctx       *Context
	rand      *rand.Rand
	paths     map[string]struct{}
	totalSize base.Byte
	sizeLimit base.Byte
}

type ephemeralRunEntry struct {
	path string
	size base.Byte
}

func directorySize(root string) (result base.Byte, err error) {
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Bail out the whole operation upon the first error.
			return err
		}
		result += base.Byte(info.Size())
		return nil
	})

	return
}

// NewEphemeralRunManager returns a new EphemeralRunManager.
func NewEphemeralRunManager(ctx *Context) *EphemeralRunManager {
	return &EphemeralRunManager{
		evictList: list.New(),
		ctx:       ctx,
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
		paths:     make(map[string]struct{}),
		sizeLimit: ctx.Config.Grader.Ephemeral.EphemeralSizeLimit,
	}
}

// Initialize goes through the past ephemeral runs in order to add them to the
// FIFO cache.
func (mgr *EphemeralRunManager) Initialize() error {
	ephemeralPath := path.Join(mgr.ctx.Config.Grader.RuntimePath, "ephemeral")

	if err := os.MkdirAll(ephemeralPath, 0755); err != nil {
		return err
	}

	dir, err := os.Open(ephemeralPath)
	if err != nil {
		return err
	}

	entryNames, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}

	var firstErr error
	for _, entryName := range entryNames {
		entryPath := path.Join(ephemeralPath, entryName)
		size, err := directorySize(entryPath)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			// This errored out. Let's try to remove the directory since it's not
			// going to participate in the total size.
			if err = os.RemoveAll(entryPath); err != nil {
				mgr.ctx.Log.Error(
					"Failed to remove directory during initialization",
					map[string]interface{}{
						"entry": entryPath,
						"err":   err,
					},
				)
			}
			continue
		}

		if err = mgr.add(&ephemeralRunEntry{
			path: entryPath,
			size: size,
		}); err != nil {
			mgr.ctx.Log.Error(
				"Failed to add entry to manager",
				map[string]interface{}{
					"entry": entryPath,
					"err":   err,
				},
			)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

func (mgr *EphemeralRunManager) tempDir() (name string, err error) {
	ephemeralPath := path.Join(mgr.ctx.Config.Grader.RuntimePath, "ephemeral")
	buf := make([]byte, 10)

	for i := 0; i < 10000; i++ {
		mgr.rand.Read(buf)
		try := path.Join(ephemeralPath, fmt.Sprintf("%x", buf))
		err = os.Mkdir(try, 0700)
		if os.IsExist(err) {
			continue
		}
		if os.IsNotExist(err) {
			if _, err := os.Stat(ephemeralPath); os.IsNotExist(err) {
				return "", err
			}
		}
		if err == nil {
			name = try
		}
		break
	}

	return
}

// SetEphemeral makes a RunContext ephemeral. It does this by setting its
// runtime path to be in the ephemeral subdirectory.
func (mgr *EphemeralRunManager) SetEphemeral(runInfo *RunInfo) (string, error) {
	artifacts, err := newEphemeralLocalGrader(&mgr.ctx.Context)
	if err != nil {
		return "", err
	}
	runInfo.Artifacts = artifacts
	return artifacts.token, nil
}

// Commit adds the files produced by the Run into the FIFO cache,
// potentially evicting older runs in the process.
func (mgr *EphemeralRunManager) Commit(runInfo *RunInfo) error {
	artifacts, ok := runInfo.Artifacts.(*localGraderArtifacts)
	if !ok {
		return fmt.Errorf("run not an ephemeral run")
	}
	size, err := directorySize(artifacts.gradeDir)
	if err != nil {
		return err
	}

	return mgr.add(&ephemeralRunEntry{
		path: artifacts.gradeDir,
		size: size,
	})
}

// Get returns the filesystem path for a previous ephemeral run, and whether or
// not it actually exists.
func (mgr *EphemeralRunManager) Get(token string) (string, bool) {
	ephemeralPath := path.Join(mgr.ctx.Config.Grader.RuntimePath, "ephemeral")

	mgr.Lock()
	defer mgr.Unlock()

	entryPath := path.Join(ephemeralPath, token)
	_, ok := mgr.paths[entryPath]
	if !ok {
		return "", false
	}
	return entryPath, true
}

func (mgr *EphemeralRunManager) String() string {
	return fmt.Sprintf("{Size=%d, Count=%d}", mgr.totalSize, len(mgr.paths))
}

func (mgr *EphemeralRunManager) add(entry *ephemeralRunEntry) error {
	mgr.Lock()
	defer mgr.Unlock()

	for mgr.evictList.Len() > 0 && mgr.totalSize+entry.size > mgr.sizeLimit {
		element := mgr.evictList.Back()
		evictedEntry := element.Value.(*ephemeralRunEntry)

		if err := os.RemoveAll(evictedEntry.path); err != nil {
			return err
		}
		mgr.totalSize -= evictedEntry.size
		mgr.evictList.Remove(element)
		delete(mgr.paths, evictedEntry.path)

		mgr.ctx.Log.Info(
			"Evicting a run",
			map[string]interface{}{
				"entry": evictedEntry,
			},
		)
	}

	mgr.evictList.PushFront(entry)
	mgr.paths[entry.path] = struct{}{}
	mgr.totalSize += entry.size

	return nil
}

// RunInfo holds the necessary data of a Run, even after the RunContext is
// gone.
type RunInfo struct {
	ID           int64
	SubmissionID int64
	GUID         string
	Contest      *string
	Problemset   *int64
	Run          *common.Run
	Result       runner.RunResult
	Artifacts    Artifacts
	Priority     QueuePriority
	PenaltyType  string
	PartialScore bool

	CreationTime time.Time
	QueueTime    time.Time
}

// RunWaitHandle allows waiting on the run to change state.
type RunWaitHandle struct {
	// A channel that will be closed once the run is ready.
	running chan struct{}

	// A channel that will be closed once the run is ready.
	ready chan struct{}
}

// Running returns a channel that will be closed when the Run is picked up
// by a runner.
func (h *RunWaitHandle) Running() <-chan struct{} {
	return h.running
}

// Ready returns a channel that will be closed when the Run is ready.
func (h *RunWaitHandle) Ready() <-chan struct{} {
	return h.ready
}

// RunContext is a wrapper around a RunInfo. This is used when a Run is sitting
// on a Queue on the grader.
type RunContext struct {
	*common.Context
	RunInfo *RunInfo

	// A flag to be able to atomically close the RunContext exactly once.
	closedFlag int32
	// A flag to be able to atomically mark the RunContext as running exactly
	// once.
	runningFlag int32
	// A reference to the Input so that it is not evicted while RunContext is
	// still active
	inputRef *common.InputRef

	attemptsLeft int
	queue        *Queue
	queueManager *QueueManager
	monitor      *InflightMonitor

	runWaitHandle *RunWaitHandle
}

// NewRunInfo returns an empty RunInfo.
func NewRunInfo() *RunInfo {
	return &RunInfo{
		Run: &common.Run{
			AttemptID: common.NewAttemptID(),
			MaxScore:  big.NewRat(1, 1),
		},
		Result:       *runner.NewRunResult("JE", &big.Rat{}),
		CreationTime: time.Now(),
		Priority:     QueuePriorityNormal,
	}
}

// Debug marks a RunContext as being for debug. This causes some additional
// logging and in C/C++ it enables AddressSanitizer. Use with caution, since
// ASan needs a relaxed sandboxing profile.
func (runCtx *RunContext) Debug() error {
	artifacts, err := newDebugLocalGrader()
	if err != nil {
		return err
	}
	runCtx.RunInfo.Artifacts = artifacts
	runCtx.RunInfo.Run.Debug = true
	return nil
}

// Close finalizes the run, stores its results in the filesystem, and releases
// any resources associated with the RunContext.
func (runCtx *RunContext) Close() {
	if atomic.SwapInt32(&runCtx.closedFlag, 1) != 0 {
		runCtx.Log.Warn("Attempting to close an already closed run", nil)
		return
	}
	defer runCtx.Context.Transaction.End()
	runCtx.Log.Info(
		"Marking run as done",
		map[string]interface{}{
			"context": runCtx,
		},
	)
	defer func() {
		runCtx.queueManager.AddEvent(&QueueEvent{
			Delta:    time.Now().Sub(runCtx.RunInfo.CreationTime),
			Priority: runCtx.RunInfo.Priority,
			Type:     QueueEventTypeManagerRemoved,
		})

		if runCtx.runWaitHandle != nil {
			close(runCtx.runWaitHandle.ready)
		}
		runCtx.queueManager.PostProcessor.PostProcess(runCtx.RunInfo)

		runCtx.Context.Close()
	}()
	if runCtx.monitor != nil {
		runCtx.monitor.Remove(runCtx.RunInfo.Run.AttemptID)
	}
	if runCtx.inputRef != nil {
		runCtx.inputRef.Release()
		runCtx.inputRef = nil
	}

	// Results
	{
		prettyPrinted, err := json.MarshalIndent(&runCtx.RunInfo.Result, "", "  ")
		if err != nil {
			runCtx.Log.Error(
				"Unable to marshal results file",
				map[string]interface{}{
					"err": err,
				},
			)
			return
		}
		err = runCtx.RunInfo.Artifacts.Put(runCtx.Context, "details.json", bytes.NewReader(prettyPrinted))
		if err != nil {
			runCtx.Log.Error(
				"Unable to write results file",
				map[string]interface{}{
					"err": err,
				},
			)
			return
		}
	}

	// Persist logs
	{
		var logsBuffer bytes.Buffer
		gz := gzip.NewWriter(&logsBuffer)
		if _, err := gz.Write(runCtx.LogBuffer()); err != nil {
			gz.Close()
			runCtx.Log.Error(
				"Unable to write log file",
				map[string]interface{}{
					"err": err,
				},
			)
			return
		}
		if err := gz.Close(); err != nil {
			runCtx.Log.Error(
				"Unable to finalize log file",
				map[string]interface{}{
					"err": err,
				},
			)
			return
		}
		err := runCtx.RunInfo.Artifacts.Put(runCtx.Context, "logs.txt.gz", &logsBuffer)
		if err != nil {
			runCtx.Log.Error(
				"Unable to create log file",
				map[string]interface{}{
					"err": err,
				},
			)
			return
		}
	}
}

// Requeue adds a RunContext back to the Queue from where it came from, if it
// has any retries left. It always adds the RunContext to the highest-priority
// queue.
func (runCtx *RunContext) Requeue(lastAttempt bool) bool {
	if runCtx.monitor != nil {
		runCtx.monitor.Remove(runCtx.RunInfo.Run.AttemptID)
	}
	runCtx.attemptsLeft--
	if runCtx.attemptsLeft <= 0 {
		runCtx.queueManager.AddEvent(&QueueEvent{
			Delta:    time.Now().Sub(runCtx.RunInfo.CreationTime),
			Priority: runCtx.RunInfo.Priority,
			Type:     QueueEventTypeAbandoned,
		})
		runCtx.Log.Error("run errored out too many times. giving up", nil)
		runCtx.Close()
		return false
	}
	if lastAttempt {
		// If this is the result of a runner successfully sending a JE verdict, it
		// _might_ be a transient problem. In any case, only attempt to run it at
		// most once more.
		runCtx.attemptsLeft = 1
	}
	runCtx.RunInfo.Run.UpdateAttemptID()
	// Since it was already ready to be executed, place it in the high-priority
	// queue.
	if !runCtx.queue.enqueue(runCtx, QueuePriorityHigh) {
		// That queue is full. We've exhausted all our options, bail out.
		runCtx.queueManager.AddEvent(&QueueEvent{
			Delta:    time.Now().Sub(runCtx.RunInfo.CreationTime),
			Priority: runCtx.RunInfo.Priority,
			Type:     QueueEventTypeAbandoned,
		})
		runCtx.Log.Error("The high-priority queue is full. giving up", nil)
		runCtx.Close()
		return false
	}
	runCtx.queueManager.AddEvent(&QueueEvent{
		Delta:    time.Now().Sub(runCtx.RunInfo.CreationTime),
		Priority: runCtx.RunInfo.Priority,
		Type:     QueueEventTypeRetried,
	})
	return true
}

func (runCtx *RunContext) String() string {
	return fmt.Sprintf(
		"RunContext{ID:%d, GUID:%s, AttemptsLeft: %d, %s}",
		runCtx.RunInfo.ID,
		runCtx.RunInfo.GUID,
		runCtx.attemptsLeft,
		runCtx.RunInfo.Run,
	)
}

// Queue represents a RunContext queue with three discrete priorities.
type Queue struct {
	Name         string
	runs         [QueueCount]chan *RunContext
	ready        chan struct{}
	queueManager *QueueManager
}

// GetRun dequeues a RunContext from the queue and adds it to the global
// InflightMonitor. This function will block if there are no RunContext objects
// in the queue.
func (queue *Queue) GetRun(
	runner string,
	monitor *InflightMonitor,
	closeNotifier <-chan bool,
) (*RunContext, <-chan struct{}, bool) {
	select {
	case <-closeNotifier:
		return nil, nil, false
	case <-queue.ready:
	}

	for i := range queue.runs {
		select {
		case runCtx := <-queue.runs[i]:
			inflight := monitor.Add(runCtx, runner)
			return runCtx, inflight.timeout, true
		default:
		}
	}
	panic("unreachable")
}

// AddRun adds a new RunContext to the current Queue.
func (queue *Queue) AddRun(
	ctx *common.Context,
	runInfo *RunInfo,
	inputRef *common.InputRef,
) error {
	runCtx := &RunContext{
		RunInfo:  runInfo,
		Context:  ctx.DebugContext(map[string]interface{}{"id": runInfo.ID}),
		inputRef: inputRef,

		attemptsLeft: ctx.Config.Grader.MaxGradeRetries,
		queueManager: queue.queueManager,
	}
	runCtx.Context.Transaction = runCtx.Context.Tracing.StartTransaction(
		"run",
		tracing.Arg{Name: "id", Value: runInfo.ID},
		tracing.Arg{Name: "submission", Value: runInfo.SubmissionID},
		tracing.Arg{Name: "guid", Value: runInfo.GUID},
	)

	runCtx.queueManager.AddEvent(&QueueEvent{
		Delta:    time.Now().Sub(runCtx.RunInfo.CreationTime),
		Priority: runCtx.RunInfo.Priority,
		Type:     QueueEventTypeManagerAdded,
	})

	if runInfo.Priority == QueuePriorityEphemeral {
		if !queue.enqueue(runCtx, runInfo.Priority) {
			runCtx.Close()
			return errors.New("The ephemeral queue is full. giving up")
		}
	} else {
		// Any run that is not intended for the ephemeral queue blocks until the
		// queue is ready to accept more runs.
		queue.enqueueBlocking(runCtx)
	}

	return nil
}

// AddWaitableRun adds a new RunContext to the current Queue, and returns a
// RunWaitHandle so the caller can wait for the run state changes. If the
// desired queue is full, there is no retry mechanism and the run is
// immediately given up.
func (queue *Queue) AddWaitableRun(
	ctx *common.Context,
	runInfo *RunInfo,
	inputRef *common.InputRef,
) (*RunWaitHandle, error) {
	runCtx := &RunContext{
		RunInfo:  runInfo,
		Context:  ctx.DebugContext(map[string]interface{}{"id": runInfo.ID}),
		inputRef: inputRef,

		attemptsLeft: ctx.Config.Grader.MaxGradeRetries,
		queueManager: queue.queueManager,
		runWaitHandle: &RunWaitHandle{
			running: make(chan struct{}),
			ready:   make(chan struct{}),
		},
	}
	runCtx.Context.Transaction = runCtx.Context.Tracing.StartTransaction(
		"run",
		tracing.Arg{Name: "id", Value: runInfo.ID},
		tracing.Arg{Name: "submission", Value: runInfo.SubmissionID},
		tracing.Arg{Name: "guid", Value: runInfo.GUID},
	)

	runCtx.queueManager.AddEvent(&QueueEvent{
		Delta:    time.Now().Sub(runCtx.RunInfo.CreationTime),
		Priority: runCtx.RunInfo.Priority,
		Type:     QueueEventTypeManagerAdded,
	})

	if !queue.enqueue(runCtx, runInfo.Priority) {
		runCtx.Close()
		return nil, errors.New("The queue is full")
	}

	return runCtx.runWaitHandle, nil
}

// enqueueBlocking adds a run to the queue, waits if needed.
func (queue *Queue) enqueueBlocking(runCtx *RunContext) {
	if runCtx == nil {
		panic("null RunContext")
	}
	runCtx.queue = queue
	queue.runs[runCtx.RunInfo.Priority] <- runCtx
	queue.ready <- struct{}{}
	runCtx.RunInfo.QueueTime = time.Now()
	queue.queueManager.AddEvent(&QueueEvent{
		Delta:    time.Now().Sub(runCtx.RunInfo.CreationTime),
		Priority: runCtx.RunInfo.Priority,
		Type:     QueueEventTypeQueueAdded,
	})
}

// enqueue adds a run to the queue, returns true if possible.
func (queue *Queue) enqueue(runCtx *RunContext, priority QueuePriority) bool {
	if runCtx == nil {
		panic("null RunContext")
	}
	runCtx.queue = queue
	select {
	case queue.runs[priority] <- runCtx:
		queue.ready <- struct{}{}
		return true
	default:
		// There is no space left in the queue.
		return false
	}
}

// InflightRun is a wrapper around a RunContext when it is handed off a queue
// and a runner has been assigned to it.
type InflightRun struct {
	runCtx       *RunContext
	runner       string
	creationTime time.Time
	connected    chan struct{}
	ready        chan struct{}
	timeout      chan struct{}
}

// InflightMonitor manages all in-flight Runs (Runs that have been picked up by
// a runner) and tracks their state in case the runner becomes unresponsive.
type InflightMonitor struct {
	sync.Mutex
	mapping        map[uint64]*InflightRun
	connectTimeout time.Duration
	readyTimeout   time.Duration
}

// RunData represents the data of a single run.
type RunData struct {
	AttemptID    uint64
	ID           int64
	GUID         string
	Queue        string
	AttemptsLeft int
	Runner       string
	Time         int64
	Elapsed      int64
}

// NewInflightMonitor returns a new InflightMonitor.
func NewInflightMonitor() *InflightMonitor {
	return &InflightMonitor{
		mapping:        make(map[uint64]*InflightRun),
		connectTimeout: time.Duration(10) * time.Minute,
		readyTimeout:   time.Duration(10) * time.Minute,
	}
}

// Add creates an InflightRun wrapper for the specified RunContext, adds it to
// the InflightMonitor, and monitors it for timeouts. A RunContext can be later
// accesssed through its attempt ID.
func (monitor *InflightMonitor) Add(
	runCtx *RunContext,
	runner string,
) *InflightRun {
	if atomic.SwapInt32(&runCtx.runningFlag, 1) == 0 && runCtx.runWaitHandle != nil {
		close(runCtx.runWaitHandle.running)
	}
	monitor.Lock()
	defer monitor.Unlock()
	inflight := &InflightRun{
		runCtx:       runCtx,
		runner:       runner,
		creationTime: time.Now(),
		connected:    make(chan struct{}, 1),
		ready:        make(chan struct{}, 1),
		timeout:      make(chan struct{}, 1),
	}
	runCtx.monitor = monitor
	monitor.mapping[runCtx.RunInfo.Run.AttemptID] = inflight
	go func() {
		defer close(inflight.timeout)

		connectTimer := time.NewTimer(monitor.connectTimeout)
		defer func() {
			if !connectTimer.Stop() {
				<-connectTimer.C
			}
		}()
		select {
		case <-inflight.connected:
		case <-connectTimer.C:
			monitor.timeout(runCtx, inflight.timeout)
			return
		}

		readyTimer := time.NewTimer(monitor.readyTimeout)
		defer func() {
			if !readyTimer.Stop() {
				<-readyTimer.C
			}
		}()
		select {
		case <-inflight.ready:
		case <-readyTimer.C:
			monitor.timeout(runCtx, inflight.timeout)
			return
		}
	}()
	return inflight
}

func (monitor *InflightMonitor) timeout(
	runCtx *RunContext,
	timeout chan<- struct{},
) {
	runCtx.Log.Warn(
		"run timed out. retrying",
		map[string]interface{}{
			"context": runCtx,
		},
	)
	runCtx.Requeue(false)
	timeout <- struct{}{}
}

// Get returns the RunContext associated with the specified attempt ID.
func (monitor *InflightMonitor) Get(attemptID uint64) (*RunContext, <-chan struct{}, bool) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight, ok := monitor.mapping[attemptID]
	if !ok {
		return nil, nil, ok
	}
	// Try to signal that the runner has connected, unless it was already
	// signalled before.
	select {
	case inflight.connected <- struct{}{}:
	default:
	}
	return inflight.runCtx, inflight.timeout, ok
}

// Remove removes the specified attempt ID from the in-flight runs and signals
// the RunContext for completion.
func (monitor *InflightMonitor) Remove(attemptID uint64) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight, ok := monitor.mapping[attemptID]
	if ok {
		inflight.runCtx.queueManager.AddEvent(&QueueEvent{
			Delta:    time.Now().Sub(inflight.runCtx.RunInfo.QueueTime),
			Priority: inflight.runCtx.RunInfo.Priority,
			Type:     QueueEventTypeQueueRemoved,
		})
		inflight.runCtx.monitor = nil
		select {
		// Try to signal that the run has been connected.
		case inflight.connected <- struct{}{}:
		default:
		}
		select {
		// Try to signal that the run has been finished.
		case inflight.ready <- struct{}{}:
		default:
		}
	}
	delete(monitor.mapping, attemptID)
}

// GetRunData returns the list of in-flight run information.
func (monitor *InflightMonitor) GetRunData() []*RunData {
	monitor.Lock()
	defer monitor.Unlock()

	data := make([]*RunData, len(monitor.mapping))
	idx := 0
	now := time.Now()
	for attemptID, inflight := range monitor.mapping {
		data[idx] = &RunData{
			AttemptID:    attemptID,
			ID:           inflight.runCtx.RunInfo.ID,
			GUID:         inflight.runCtx.RunInfo.GUID,
			Queue:        inflight.runCtx.queue.Name,
			AttemptsLeft: inflight.runCtx.attemptsLeft,
			Runner:       inflight.runner,
			Time:         inflight.creationTime.Unix(),
			Elapsed:      now.Sub(inflight.creationTime).Nanoseconds(),
		}
		idx++
	}

	return data
}

// MarshalJSON returns a JSON representation of the InflightMonitor.
func (monitor *InflightMonitor) MarshalJSON() ([]byte, error) {
	return json.MarshalIndent(monitor.GetRunData(), "", "  ")
}

type runPostProcessorListener struct {
	listener *chan<- *RunInfo
	added    *chan struct{}
}

// A RunPostProcessor broadcasts the events of runs that have been finished to
// all registered listeners.
type RunPostProcessor struct {
	finishedRuns chan *RunInfo
	listenerChan chan runPostProcessorListener
	listeners    []chan<- *RunInfo
}

// NewRunPostProcessor returns a new RunPostProcessor.
func NewRunPostProcessor() *RunPostProcessor {
	return &RunPostProcessor{
		finishedRuns: make(chan *RunInfo, 1),
		listenerChan: make(chan runPostProcessorListener, 1),
		listeners:    make([]chan<- *RunInfo, 0),
	}
}

// AddListener adds a channel that will be notified for every Run that has
// finished.
func (postProcessor *RunPostProcessor) AddListener(c chan<- *RunInfo) {
	added := make(chan struct{}, 0)
	postProcessor.listenerChan <- runPostProcessorListener{
		listener: &c,
		added:    &added,
	}
	select {
	case <-added:
	}
}

// PostProcess queues the provided run for post-processing. All the registered
// listeners will be notified about this run.
func (postProcessor *RunPostProcessor) PostProcess(run *RunInfo) {
	postProcessor.finishedRuns <- run
}

func (postProcessor *RunPostProcessor) run() {
	for {
		select {
		case wrappedListener := <-postProcessor.listenerChan:
			postProcessor.listeners = append(
				postProcessor.listeners,
				*wrappedListener.listener,
			)
			close(*wrappedListener.added)
		case run, ok := <-postProcessor.finishedRuns:
			if !ok {
				for _, listener := range postProcessor.listeners {
					close(listener)
				}
				return
			}
			for _, listener := range postProcessor.listeners {
				listener <- run
			}
		}
	}
}

// Close notifies the RunPostProcessor goroutine that there is no more work to
// be done.
func (postProcessor *RunPostProcessor) Close() {
	close(postProcessor.finishedRuns)
}

// QueueEvent represents an event that happens from the QueueManager's perspective.
type QueueEvent struct {
	Delta    time.Duration
	Priority QueuePriority
	Type     QueueEventType
}

type queueEventListener struct {
	listener *chan<- *QueueEvent
	added    *chan struct{}
}

// QueueManager is an expvar-friendly manager for Queues.
type QueueManager struct {
	sync.Mutex
	PostProcessor *RunPostProcessor

	mapping       map[string]*Queue
	channelLength int
	events        chan *QueueEvent
	listenerChan  chan queueEventListener
	listeners     []chan<- *QueueEvent
}

// QueueInfo has information about one queue.
type QueueInfo struct {
	Lengths [QueueCount]int
}

// NewQueueManager creates a new QueueManager.
func NewQueueManager(channelLength int, graderRuntimePath string) *QueueManager {
	manager := &QueueManager{
		PostProcessor: NewRunPostProcessor(),
		mapping:       make(map[string]*Queue),
		channelLength: channelLength,
		events:        make(chan *QueueEvent, 1),
		listenerChan:  make(chan queueEventListener, 1),
		listeners:     make([]chan<- *QueueEvent, 0),
	}
	manager.Add(DefaultQueueName)
	go manager.run()
	go manager.PostProcessor.run()
	return manager
}

// Add creates a new queue or fetches a previously created queue with the
// specified name and returns it.
func (manager *QueueManager) Add(name string) *Queue {
	queue := &Queue{
		Name:         name,
		ready:        make(chan struct{}, QueueCount*manager.channelLength),
		queueManager: manager,
	}
	for r := range queue.runs {
		queue.runs[r] = make(chan *RunContext, manager.channelLength)
	}
	manager.Lock()
	defer manager.Unlock()
	manager.mapping[name] = queue
	return queue
}

// Get gets the queue with the specified name.
func (manager *QueueManager) Get(name string) (*Queue, error) {
	manager.Lock()
	defer manager.Unlock()

	queue, ok := manager.mapping[name]
	if !ok {
		return nil, fmt.Errorf("cannot find queue %q", name)
	}
	return queue, nil
}

// GetQueueInfo returns the length of all the queues.
func (manager *QueueManager) GetQueueInfo() map[string]QueueInfo {
	manager.Lock()
	defer manager.Unlock()

	queues := make(map[string]QueueInfo)
	for name, queue := range manager.mapping {
		queues[name] = QueueInfo{
			Lengths: [QueueCount]int{
				len(queue.runs[0]),
				len(queue.runs[1]),
				len(queue.runs[2]),
				len(queue.runs[3]),
			},
		}
	}
	return queues
}

// MarshalJSON returns a JSON representation of the queue lengths for reporting
// purposes.
func (manager *QueueManager) MarshalJSON() ([]byte, error) {
	return json.MarshalIndent(manager.GetQueueInfo(), "", "  ")
}

// AddEventListener registers a listener for event notifications.
func (manager *QueueManager) AddEventListener(c chan<- *QueueEvent) {
	added := make(chan struct{}, 0)
	manager.listenerChan <- queueEventListener{
		listener: &c,
		added:    &added,
	}
	select {
	case <-added:
	}
}

// AddEvent adds an event and broadcasts it to all the listeners.
func (manager *QueueManager) AddEvent(event *QueueEvent) {
	manager.events <- event
}

// Close terminates the event listener goroutine.
func (manager *QueueManager) Close() {
	close(manager.events)
	manager.PostProcessor.Close()
}

func (manager *QueueManager) run() {
	for {
		select {
		case wrappedListener := <-manager.listenerChan:
			manager.listeners = append(
				manager.listeners,
				*wrappedListener.listener,
			)
			close(*wrappedListener.added)
		case event, ok := <-manager.events:
			if !ok {
				for _, listener := range manager.listeners {
					close(listener)
				}
				return
			}
			for _, listener := range manager.listeners {
				listener <- event
			}
		}
	}
}
