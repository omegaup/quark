package grader

import (
	"database/sql"
	"encoding/json"
	"expvar"
	"github.com/lhchavez/quark/common"
	"io/ioutil"
	"path"
	"reflect"
	"sync"
	"time"
)

var (
	GlobalInflightMonitor = InflightMonitor{
		mapping: make(map[uint64]*InflightRun),
	}
	GlobalQueueMonitor = QueueMonitor{
		mapping: make(map[string]*Queue),
	}
)

func init() {
	expvar.Publish("inflight_runs", &GlobalInflightMonitor)
	expvar.Publish("queues", &GlobalQueueMonitor)
}

// RunContext is a wrapper around a Run. This is used when a Run is sitting on
// a Queue on the grader.
type RunContext struct {
	Run   *common.Run
	Input common.Input
	tries int
	queue *Queue
	pool  *Pool
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
		run.InputHash,
		NewGraderInputFactory(run, &ctx.Config),
	)
	if err != nil {
		return nil, err
	}

	runctx := &RunContext{
		Run:   run,
		Input: input,
		tries: ctx.Config.Grader.MaxGradeRetries,
	}
	return runctx, nil
}

func newRun(ctx *Context, id int64) (*common.Run, error) {
	run := &common.Run{
		ID: common.NewRunID(),
	}
	var contestName sql.NullString
	var contestPoints sql.NullFloat64
	err := ctx.DB.QueryRow(
		`SELECT
			s.guid, c.alias, s.language, p.alias, pv.hash, cp.points
		FROM
			Submissions s
		INNER JOIN
			Problems p ON p.problem_id = s.problem_id
		INNER JOIN
			Problem_Versions pv ON pv.version_id = p.current_version
		LEFT JOIN
			Contests c ON c.contest_id = s.contest_id
		LEFT JOIN
			Contest_Problems cp ON cp.problem_id = s.problem_id AND
			cp.contest_id = s.contest_id
		WHERE
			s.submission_id = ?;`, id).Scan(
		&run.GUID, &contestName, &run.Language, &run.Problem.Name,
		&run.InputHash, &contestPoints)
	if err != nil {
		return nil, err
	}
	if contestName.Valid {
		run.Contest = &contestName.String
	}
	if contestPoints.Valid {
		run.Problem.Points = &contestPoints.Float64
	}
	contents, err := ioutil.ReadFile(
		path.Join(
			ctx.Config.Grader.RuntimePath, "submissions", run.GUID[:2], run.GUID[2:],
		),
	)
	if err != nil {
		return nil, err
	}
	run.Source = string(contents)
	return run, nil
}

// Requeue adds a RunContext back to the Queue from where it came from, if it
// has any retries left. It always adds the RunContext to the highest-priority
// queue.
func (run *RunContext) Requeue() bool {
	run.tries -= 1
	if run.tries <= 0 {
		return false
	}
	run.Run.UpdateID()
	// Since it was already ready to be executed, place it in the high-priority
	// queue.
	run.queue.Enqueue(run, 0)
	return true
}

// Queue represents a RunContext queue with three discrete priorities.
type Queue struct {
	Name  string
	runs  [3]chan *RunContext
	ready chan struct{}
}

func NewQueue(name string, channelLength int) *Queue {
	queue := &Queue{
		Name:  name,
		ready: make(chan struct{}, channelLength),
	}
	for r := range queue.runs {
		queue.runs[r] = make(chan *RunContext, channelLength)
	}
	GlobalQueueMonitor.Lock()
	defer GlobalQueueMonitor.Unlock()
	GlobalQueueMonitor.mapping[name] = queue
	return queue
}

// TryGetRun goes through the channels in order of priority, and if one of them
// has something ready, it returns it.
func (queue *Queue) TryGetRun() (*RunContext, bool) {
	for i := range queue.runs {
		select {
		case run := <-queue.runs[i]:
			return run, true
		default:
		}
	}
	return nil, false
}

// GetRun dequeues a RunContext from the queue and adds it to the global
// InflightMonitor. This function will block if there are no RunContext objects
// in the queue.
func (queue *Queue) GetRun(
	runner string,
	closeNotifier <-chan bool,
	timeout chan<- bool,
) (*RunContext, bool) {
	select {
	case <-closeNotifier:
		return nil, false
	case <-queue.ready:
	}

	if run, ok := queue.TryGetRun(); ok {
		GlobalInflightMonitor.Add(run, runner, timeout)
		return run, true
	}
	panic("unreachable")
}

// Close closes all of the Queue's run queues and drains them into output.
func (queue *Queue) Close(output chan<- *RunContext) {
	close(queue.ready)
	for run := range queue.runs[0] {
		output <- run
	}
	close(queue.runs[0])
	for run := range queue.runs[1] {
		output <- run
	}
	close(queue.runs[1])
	for run := range queue.runs[2] {
		output <- run
	}
	close(queue.runs[2])
	GlobalQueueMonitor.Lock()
	defer GlobalQueueMonitor.Unlock()
	delete(GlobalQueueMonitor.mapping, queue.Name)
}

// Enqueue adds a run to the queue.
func (queue *Queue) Enqueue(run *RunContext, idx int) {
	if run == nil {
		panic("null RunContext")
	}
	run.queue = queue
	queue.runs[idx] <- run
	queue.ready <- struct{}{}
}

// Pool is a collection of Queue objects. This is used when several Queues are
// to be consumed by a set of runners. For instance, there could be a Pool for
// Raspberry Pi runners and another one for x86_64 runners.
type Pool struct {
	Name   string
	queues []*Queue
	cases  []reflect.SelectCase
}

// NewPool creates a Pool with the specified Queues assigned to it.
func NewPool(name string, queues []*Queue) *Pool {
	pool := &Pool{
		Name:   name,
		queues: queues,
		cases:  make([]reflect.SelectCase, len(queues)),
	}
	for i := range pool.queues {
		pool.cases[i].Dir = reflect.SelectRecv
		pool.cases[i].Chan = reflect.ValueOf(pool.queues[i].ready)
	}
	return pool
}

// GetRun behaves exactly like Queue's GetRun.
func (pool *Pool) GetRun(
	runner string,
	closeNotifier <-chan bool,
	timeout chan<- bool,
) (*RunContext, bool) {
	for i := range pool.queues {
		if _, ok := <-pool.queues[i].ready; ok {
			ctx, ok := pool.queues[i].TryGetRun()
			if !ok {
				panic("Unexpected !ok")
			}
			GlobalInflightMonitor.Add(ctx, runner, timeout)
			return ctx, true
		}
	}

	// Otherwise wait until one of them has something.
	// TODO(lhchavez): Add the closeNotifier to the Select.
	chosen, _, _ := reflect.Select(pool.cases)
	return pool.queues[chosen].GetRun(runner, closeNotifier, timeout)
}

// InflightRun is a wrapper around a RunContext when it is handed off a queue
// and a runner has been assigned to it.
type InflightRun struct {
	run          *RunContext
	runner       string
	creationTime int64
	connected    chan struct{}
	ready        chan struct{}
}

// InflightMonitor manages all in-flight Runs (Runs that have been picked up by
// a runner) and tracks their state in case the runner becomes unresponsive.
type InflightMonitor struct {
	sync.Mutex
	mapping map[uint64]*InflightRun
}

// Add creates an InflightRun wrapper for the specified RunContext, adds it to
// the InflightMonitor, and monitors it for timeouts. A RunContext can be later
// accesssed through its attempt ID.
func (monitor *InflightMonitor) Add(
	run *RunContext,
	runner string,
	timeout chan<- bool,
) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight := &InflightRun{
		run:          run,
		runner:       runner,
		creationTime: time.Now().Unix(),
		connected:    make(chan struct{}, 1),
		ready:        make(chan struct{}, 1),
	}
	monitor.mapping[run.Run.ID] = inflight
	go func() {
		select {
		case <-inflight.connected:
			select {
			case <-inflight.ready:
				timeout <- false
			case <-time.After(time.Duration(10) * time.Minute):
				timeout <- true
			}
		case <-time.After(time.Duration(2) * time.Second):
			timeout <- true
		}
	}()
}

// Get returns the RunContext associated with the specified attempt ID.
func (monitor *InflightMonitor) Get(id uint64) (*RunContext, bool) {
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
	return inflight.run, ok
}

// Remove removes the specified attempt ID from the in-flight runs and signals
// the RunContext for completion.
func (monitor *InflightMonitor) Remove(id uint64) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight, ok := monitor.mapping[id]
	if ok {
		select {
		// Try to signal that the run has been finished.
		case inflight.ready <- struct{}{}:
		default:
		}
	}
	delete(monitor.mapping, id)
}

func (monitor *InflightMonitor) String() string {
	monitor.Lock()
	defer monitor.Unlock()

	type runData struct {
		ID           uint64
		GUID         string
		Queue        string
		AttemptsLeft int
		Runner       string
		Time         int64
		Elapsed      string
	}

	data := make([]*runData, len(monitor.mapping))
	idx := 0
	now := time.Now()
	for id, inflight := range monitor.mapping {
		data[idx] = &runData{
			ID:           id,
			GUID:         inflight.run.Run.GUID,
			Queue:        inflight.run.queue.Name,
			AttemptsLeft: inflight.run.tries,
			Runner:       inflight.runner,
			Time:         inflight.creationTime,
			Elapsed:      now.Sub(time.Unix(inflight.creationTime, 0)).String(),
		}
		idx += 1
	}

	buf, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(buf)
}

// QueueMonitor is an expvar-friendly monitor for Queues.
type QueueMonitor struct {
	sync.Mutex
	mapping map[string]*Queue
}

func (monitor *QueueMonitor) String() string {
	monitor.Lock()
	defer monitor.Unlock()

	type queueInfo [3]int
	queues := make(map[string]queueInfo)
	for name, queue := range monitor.mapping {
		queues[name] = [3]int{
			len(queue.runs[0]),
			len(queue.runs[1]),
			len(queue.runs[2]),
		}
	}

	buf, err := json.MarshalIndent(queues, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(buf)
}
