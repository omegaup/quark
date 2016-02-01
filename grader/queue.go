package grader

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inconshreveable/log15"
	"github.com/lhchavez/quark/common"
	"io/ioutil"
	"path"
	"sync"
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
	Input       common.Input

	// These fields are there so that the RunContext can be used as a normal
	// Context.
	Log            log15.Logger
	EventCollector common.EventCollector
	EventFactory   *common.EventFactory
	Config         *common.Config

	creationTime int64
	tries        int
	queue        *Queue
	context      *common.Context
	monitor      *InflightMonitor
}

// newRunContext creates a RunContext from its database id.
func newRunContext(
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
	run.Input = input
	run.context = ctx.Context.DebugContext()

	run.Config = &run.context.Config
	run.Log = run.context.Log
	run.EventCollector = run.context.EventCollector
	run.EventFactory = run.context.EventFactory

	return run, nil
}

func newRun(ctx *Context, id int64) (*RunContext, error) {
	runCtx := &RunContext{
		Run: &common.Run{
			AttemptID: common.NewAttemptID(),
			MaxScore:  1.0,
		},
		ID:           id,
		creationTime: time.Now().Unix(),
		tries:        ctx.Config.Grader.MaxGradeRetries,
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

func (run *RunContext) Close() {
	if run.monitor != nil {
		run.monitor.Remove(run.Run.AttemptID)
	}
	// TODO(lhchavez): Persist logs and tracing data.
}

// Requeue adds a RunContext back to the Queue from where it came from, if it
// has any retries left. It always adds the RunContext to the highest-priority
// queue.
func (run *RunContext) Requeue() bool {
	if run.monitor != nil {
		run.monitor.Remove(run.Run.AttemptID)
	}
	run.tries -= 1
	if run.tries <= 0 {
		return false
	}
	run.Run.UpdateAttemptID()
	// Since it was already ready to be executed, place it in the high-priority
	// queue.
	run.queue.enqueue(run, 0)
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

func (queue *Queue) AddRun(
	ctx *Context,
	id int64,
	inputManager *common.InputManager,
) (*RunContext, error) {
	run, err := newRunContext(ctx, id, inputManager)
	if err != nil {
		return nil, err
	}
	// TODO(lhchavez): Add async events for queue operations.
	// Add new runs to the normal priority by default.
	queue.enqueue(run, 1)
	return run, nil
}

// enqueue adds a run to the queue.
func (queue *Queue) enqueue(run *RunContext, idx int) {
	if run == nil {
		panic("null RunContext")
	}
	run.queue = queue
	queue.runs[idx] <- run
	queue.ready <- struct{}{}
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

func NewInflightMonitor() *InflightMonitor {
	return &InflightMonitor{
		mapping:        make(map[uint64]*InflightRun),
		connectTimeout: time.Duration(2) * time.Second,
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
	if !run.Requeue() {
		run.context.Log.Error("run timed out too many times. giving up")
	}
	timeout <- struct{}{}
}

// Get returns the RunContext associated with the specified attempt ID.
func (monitor *InflightMonitor) Get(id uint64) (*RunContext, <-chan struct{}, bool) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight, ok := monitor.mapping[id]
	if ok {
		// Try to signal that the runner has connected, unless it was already
		// signalled before.
		select {
		case inflight.connected <- struct{}{}:
		default:
		}
	}
	return inflight.run, inflight.timeout, ok
}

// Remove removes the specified attempt ID from the in-flight runs and signals
// the RunContext for completion.
func (monitor *InflightMonitor) Remove(id uint64) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight, ok := monitor.mapping[id]
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
	delete(monitor.mapping, id)
}

func (monitor *InflightMonitor) MarshalJSON() ([]byte, error) {
	monitor.Lock()
	defer monitor.Unlock()

	type runData struct {
		AttemptID    uint64
		ID           int64
		GUID         string
		Queue        string
		AttemptsLeft int
		Runner       string
		Time         int64
		Elapsed      int64
	}

	data := make([]*runData, len(monitor.mapping))
	idx := 0
	now := time.Now()
	for attemptId, inflight := range monitor.mapping {
		data[idx] = &runData{
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

	return json.MarshalIndent(data, "", "  ")
}

// QueueManager is an expvar-friendly manager for Queues.
type QueueManager struct {
	sync.Mutex
	mapping       map[string]*Queue
	channelLength int
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
		ready: make(chan struct{}, manager.channelLength),
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

func (manager *QueueManager) MarshalJSON() ([]byte, error) {
	manager.Lock()
	defer manager.Unlock()

	type queueInfo [3]int
	queues := make(map[string]queueInfo)
	for name, queue := range manager.mapping {
		queues[name] = [3]int{
			len(queue.runs[0]),
			len(queue.runs[1]),
			len(queue.runs[2]),
		}
	}

	return json.MarshalIndent(queues, "", "  ")
}
