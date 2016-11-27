package grader

import (
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inconshreveable/log15"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/runner"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// RunContext is a wrapper around a Run. This is used when a Run is sitting on
// a Queue on the grader.
type RunContext struct {
	ID          int64
	GUID        string
	Contest     *string
	ProblemName string
	Run         *common.Run
	Result      runner.RunResult

	// These fields are there so that the RunContext can be used as a normal
	// Context.
	Log            log15.Logger
	EventCollector common.EventCollector
	EventFactory   *common.EventFactory
	Config         *common.Config

	// A flag to be able to atomically close the RunContext exactly once.
	closed int32
	// A reference to the Input so that it is not evicted while RunContext is
	// still active
	input common.Input

	gradeDir     string
	creationTime int64
	tries        int
	queue        *Queue
	context      *common.Context
	monitor      *InflightMonitor

	// A channel that will be closed once the run is ready.
	ready chan struct{}
}

// NewRunContext creates a RunContext from its database id.
func NewRunContext(
	ctx *Context,
	id int64,
	inputManager *common.InputManager,
) (*RunContext, error) {
	run, err := newRun(ctx, id)
	if err != nil {
		return nil, err
	}
	input, err := inputManager.Add(
		run.Run.InputHash,
		NewGraderInputFactory(run.ProblemName, &ctx.Config),
	)
	if err != nil {
		return nil, err
	}
	run.input = input
	run.context = ctx.Context.DebugContext("id", id)

	run.Config = &run.context.Config
	run.Log = run.context.Log
	run.EventCollector = run.context.EventCollector
	run.EventFactory = run.context.EventFactory

	run.gradeDir = path.Join(
		run.Config.Grader.RuntimePath,
		"grade",
		fmt.Sprintf("%02d", run.ID%100),
		fmt.Sprintf("%d", run.ID),
	)

	return run, nil
}

func newRun(ctx *Context, id int64) (*RunContext, error) {
	runCtx := &RunContext{
		Run: &common.Run{
			AttemptID: common.NewAttemptID(),
			MaxScore:  1.0,
		},
		Result: runner.RunResult{
			Verdict: "JE",
		},
		ID:           id,
		creationTime: time.Now().Unix(),
		tries:        ctx.Config.Grader.MaxGradeRetries,
		ready:        make(chan struct{}),
	}
	var contestName sql.NullString
	var contestPoints sql.NullFloat64
	err := ctx.DB.QueryRow(
		`SELECT
			s.guid, c.alias, s.language, p.alias, pv.hash, cp.points
		FROM
			Runs r
		INNER JOIN
			Submissions s ON r.submission_id = s.submission_id
		INNER JOIN
			Problems p ON p.problem_id = s.problem_id
		INNER JOIN
			Problem_Versions pv ON pv.version_id = r.version_id
		LEFT JOIN
			Contests c ON c.contest_id = s.contest_id
		LEFT JOIN
			Contest_Problems cp ON cp.problem_id = s.problem_id AND
			cp.contest_id = s.contest_id
		WHERE
			r.run_id = ?;`, id).Scan(
		&runCtx.GUID, &contestName, &runCtx.Run.Language, &runCtx.ProblemName,
		&runCtx.Run.InputHash, &contestPoints)
	if err != nil {
		return nil, err
	}
	if contestName.Valid {
		runCtx.Contest = &contestName.String
	}
	if contestPoints.Valid {
		runCtx.Run.MaxScore = contestPoints.Float64
	}
	runCtx.Result.MaxScore = runCtx.Run.MaxScore
	contents, err := ioutil.ReadFile(
		path.Join(
			ctx.Config.Grader.RuntimePath,
			"submissions",
			runCtx.GUID[:2],
			runCtx.GUID[2:],
		),
	)
	if err != nil {
		return nil, err
	}
	runCtx.Run.Source = string(contents)
	return runCtx, nil
}

func (run *RunContext) GradeDir() string {
	return run.gradeDir
}

func (run *RunContext) Debug() error {
	var err error
	if run.gradeDir, err = ioutil.TempDir("", "grade"); err != nil {
		return err
	}
	run.Run.Debug = true
	return nil
}

func (run *RunContext) Close() {
	if atomic.SwapInt32(&run.closed, 1) != 0 {
		run.Log.Warn("Attempting to close an already closed run")
		return
	}
	if run.monitor != nil {
		run.monitor.Remove(run.Run.AttemptID)
	}
	if run.input != nil {
		run.input.Release(run.input)
		run.input = nil
	}
	gradeDir := run.GradeDir()
	if err := os.MkdirAll(gradeDir, 0755); err != nil {
		run.Log.Error("Unable to create grade dir", "err", err)
		return
	}

	// Results
	{
		fd, err := os.Create(path.Join(gradeDir, "details.json"))
		if err != nil {
			run.Log.Error("Unable to create details.json file", "err", err)
			return
		}
		defer fd.Close()
		prettyPrinted, err := json.MarshalIndent(&run.Result, "", "  ")
		if err != nil {
			run.Log.Error("Unable to marshal results file", "err", err)
			return
		}
		if _, err := fd.Write(prettyPrinted); err != nil {
			run.Log.Error("Unable to write results file", "err", err)
			return
		}
	}

	// Persist logs
	{
		fd, err := os.Create(path.Join(gradeDir, "logs.txt.gz"))
		if err != nil {
			run.Log.Error("Unable to create log file", "err", err)
			return
		}
		defer fd.Close()
		gz := gzip.NewWriter(fd)
		if _, err := gz.Write(run.context.LogBuffer()); err != nil {
			run.Log.Error("Unable to write log file", "err", err)
			return
		}
		if err := gz.Close(); err != nil {
			run.Log.Error("Unable to finalize log file", "err", err)
			return
		}
	}

	// Persist tracing info
	{
		fd, err := os.Create(path.Join(gradeDir, "tracing.json.gz"))
		if err != nil {
			run.Log.Error("Unable to create tracing file", "err", err)
			return
		}
		defer fd.Close()
		gz := gzip.NewWriter(fd)
		if _, err := gz.Write(run.context.TraceBuffer()); err != nil {
			run.Log.Error("Unable to upload traces", "err", err)
			return
		}
		if err := gz.Close(); err != nil {
			run.Log.Error("Unable to finalize traces", "err", err)
			return
		}
	}

	close(run.ready)
}

func (run *RunContext) AppendRunnerLogs(runnerName string, contents []byte) {
	run.context.AppendLogSection(runnerName, contents)
}

// Requeue adds a RunContext back to the Queue from where it came from, if it
// has any retries left. It always adds the RunContext to the highest-priority
// queue.
func (run *RunContext) Requeue(lastAttempt bool) bool {
	if run.monitor != nil {
		run.monitor.Remove(run.Run.AttemptID)
	}
	run.tries -= 1
	if run.tries <= 0 {
		run.Close()
		return false
	}
	if lastAttempt {
		// If this is the result of a runner successfully sending a JE verdict, it
		// _might_ be a transient problem. In any case, only attempt to run it at
		// most once more.
		run.tries = 1
	}
	run.Run.UpdateAttemptID()
	// Since it was already ready to be executed, place it in the high-priority
	// queue.
	if !run.queue.enqueue(run, 0) {
		// That queue is full. We've exhausted all our options, bail out.
		run.Close()
		return false
	}
	return true
}

func (run *RunContext) String() string {
	return fmt.Sprintf(
		"RunContext{ID:%d GUID:%s ProblemName:%s, %s}",
		run.ID,
		run.GUID,
		run.ProblemName,
		run.Run,
	)
}

func (run *RunContext) Ready() <-chan struct{} {
	return run.ready
}

// Queue represents a RunContext queue with three discrete priorities.
type Queue struct {
	Name  string
	runs  [3]chan *RunContext
	ready chan struct{}
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
		case run := <-queue.runs[i]:
			inflight := monitor.Add(run, runner)
			return run, inflight.timeout, true
		default:
		}
	}
	panic("unreachable")
}

func (queue *Queue) AddRun(run *RunContext) {
	// TODO(lhchavez): Add async events for queue operations.
	// Add new runs to the normal priority by default.
	queue.enqueueBlocking(run, 1)
}

// enqueueBlocking adds a run to the queue, waits if needed.
func (queue *Queue) enqueueBlocking(run *RunContext, idx int) {
	if run == nil {
		panic("null RunContext")
	}
	run.queue = queue
	queue.runs[idx] <- run
	queue.ready <- struct{}{}
}

// enqueue adds a run to the queue, returns true if possible.
func (queue *Queue) enqueue(run *RunContext, idx int) bool {
	if run == nil {
		panic("null RunContext")
	}
	run.queue = queue
	select {
	case queue.runs[idx] <- run:
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
	run          *RunContext
	runner       string
	creationTime int64
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
	run *RunContext,
	runner string,
) *InflightRun {
	monitor.Lock()
	defer monitor.Unlock()
	inflight := &InflightRun{
		run:          run,
		runner:       runner,
		creationTime: time.Now().Unix(),
		connected:    make(chan struct{}, 1),
		ready:        make(chan struct{}, 1),
		timeout:      make(chan struct{}, 1),
	}
	run.monitor = monitor
	monitor.mapping[run.Run.AttemptID] = inflight
	go func() {
		select {
		case <-inflight.connected:
			select {
			case <-inflight.ready:
			case <-time.After(monitor.readyTimeout):
				monitor.timeout(run, inflight.timeout)
			}
		case <-time.After(monitor.connectTimeout):
			monitor.timeout(run, inflight.timeout)
		}
		close(inflight.timeout)
	}()
	return inflight
}

func (monitor *InflightMonitor) timeout(
	run *RunContext,
	timeout chan<- struct{},
) {
	run.context.Log.Error("run timed out. retrying", "context", run)
	if !run.Requeue(false) {
		run.context.Log.Error("run timed out too many times. giving up")
	}
	timeout <- struct{}{}
}

// Get returns the RunContext associated with the specified attempt ID.
func (monitor *InflightMonitor) Get(attemptID uint64) (*RunContext, <-chan struct{}, bool) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight, ok := monitor.mapping[attemptID]
	if ok {
		// Try to signal that the runner has connected, unless it was already
		// signalled before.
		select {
		case inflight.connected <- struct{}{}:
		default:
		}
		return inflight.run, inflight.timeout, ok
	}
	return nil, nil, ok
}

// Remove removes the specified attempt ID from the in-flight runs and signals
// the RunContext for completion.
func (monitor *InflightMonitor) Remove(attemptID uint64) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight, ok := monitor.mapping[attemptID]
	if ok {
		inflight.run.monitor = nil
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

func (monitor *InflightMonitor) GetRunData() []*RunData {
	monitor.Lock()
	defer monitor.Unlock()

	data := make([]*RunData, len(monitor.mapping))
	idx := 0
	now := time.Now()
	for attemptId, inflight := range monitor.mapping {
		data[idx] = &RunData{
			AttemptID:    attemptId,
			ID:           inflight.run.ID,
			GUID:         inflight.run.GUID,
			Queue:        inflight.run.queue.Name,
			AttemptsLeft: inflight.run.tries,
			Runner:       inflight.runner,
			Time:         inflight.creationTime,
			Elapsed:      now.Sub(time.Unix(inflight.creationTime, 0)).Nanoseconds(),
		}
		idx += 1
	}

	return data
}

func (monitor *InflightMonitor) MarshalJSON() ([]byte, error) {
	return json.MarshalIndent(monitor.GetRunData(), "", "  ")
}

// QueueManager is an expvar-friendly manager for Queues.
type QueueManager struct {
	sync.Mutex
	mapping       map[string]*Queue
	channelLength int
}

// QueueInfo has information about one queue.
type QueueInfo struct {
	Lengths [3]int
}

func NewQueueManager(channelLength int) *QueueManager {
	manager := &QueueManager{
		mapping:       make(map[string]*Queue),
		channelLength: channelLength,
	}
	manager.Add("default")
	return manager
}

func (manager *QueueManager) Add(name string) *Queue {
	queue := &Queue{
		Name:  name,
		ready: make(chan struct{}, 3*manager.channelLength),
	}
	for r := range queue.runs {
		queue.runs[r] = make(chan *RunContext, manager.channelLength)
	}
	manager.Lock()
	defer manager.Unlock()
	manager.mapping[name] = queue
	return queue
}

func (manager *QueueManager) Get(name string) (*Queue, error) {
	manager.Lock()
	defer manager.Unlock()

	queue, ok := manager.mapping[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("cannot find queue %q", name))
	}
	return queue, nil
}

func (manager *QueueManager) GetQueueInfo() map[string]QueueInfo {
	manager.Lock()
	defer manager.Unlock()

	queues := make(map[string]QueueInfo)
	for name, queue := range manager.mapping {
		queues[name] = QueueInfo{
			Lengths: [3]int{
				len(queue.runs[0]),
				len(queue.runs[1]),
				len(queue.runs[2]),
			},
		}
	}
	return queues
}

func (manager *QueueManager) MarshalJSON() ([]byte, error) {
	return json.MarshalIndent(manager.GetQueueInfo(), "", "  ")
}
