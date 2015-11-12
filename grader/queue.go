package grader

import (
	"database/sql"
	"github.com/lhchavez/quark/common"
	"io/ioutil"
	"path"
	"reflect"
	"sync"
	"time"
)

var GlobalInflightMonitor = InflightMonitor{
	mapping: make(map[uint64]*InflightRun),
}

type InflightRun struct {
	run   *RunContext
	ready chan struct{}
}

type InflightMonitor struct {
	sync.Mutex
	mapping map[uint64]*InflightRun
}

type RunContext struct {
	Run   *common.Run
	Input common.Input
	tries int
	queue *Queue
	pool  *Pool
}

type Queue struct {
	Name  string
	runs  [3]chan *RunContext
	ready chan struct{}
}

func newRun(id int64, ctx *Context) (*common.Run, error) {
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
	contents, err := ioutil.ReadFile(path.Join(ctx.Config.Grader.RuntimePath,
		"submissions", run.GUID[:2], run.GUID[2:]))
	if err != nil {
		return nil, err
	}
	run.Source = string(contents)
	return run, nil
}

func NewRunContext(id int64, ctx *Context) (*RunContext, error) {
	run, err := newRun(id, ctx)
	if err != nil {
		return nil, err
	}
	input, err := common.DefaultInputManager.Add(run.InputHash,
		NewGraderInputFactory(run, &ctx.Config))
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

func (run *RunContext) Requeue() bool {
	run.tries -= 1
	if run.tries <= 0 {
		return false
	}
	run.Run.UpdateID()
	// Since it was already ready to be executed, place it in the high-priority queue.
	run.queue.Enqueue(run, 0)
	return true
}

func NewQueue(name string, channelLength int) *Queue {
	queue := &Queue{
		Name:  name,
		ready: make(chan struct{}, channelLength),
	}
	for r := range queue.runs {
		queue.runs[r] = make(chan *RunContext, channelLength)
	}
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

// GetRun
func (queue *Queue) GetRun(closeNotifier <-chan bool,
	timeout chan<- bool) (*RunContext, bool) {
	select {
	case <-closeNotifier:
		return nil, false
	case <-queue.ready:
	}

	if run, ok := queue.TryGetRun(); ok {
		GlobalInflightMonitor.Add(run, timeout)
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

type Pool struct {
	Name   string
	queues []*Queue
	cases  []reflect.SelectCase
}

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

func (pool *Pool) GetRun(closeNotifier <-chan bool,
	timeout chan<- bool) (*RunContext, bool) {
	for i := range pool.queues {
		if _, ok := <-pool.queues[i].ready; ok {
			ctx, ok := pool.queues[i].TryGetRun()
			if !ok {
				panic("Unexpected !ok")
			}
			GlobalInflightMonitor.Add(ctx, timeout)
			return ctx, true
		}
	}

	// Otherwise wait until one of them has something.
	// TODO(lhchavez): Add the closeNotifier to the Select.
	chosen, _, _ := reflect.Select(pool.cases)
	return pool.queues[chosen].GetRun(closeNotifier, timeout)
}

func (monitor *InflightMonitor) Add(run *RunContext, timeout chan<- bool) {
	monitor.Lock()
	defer monitor.Unlock()
	inflight := &InflightRun{
		run:   run,
		ready: make(chan struct{}, 1),
	}
	monitor.mapping[run.Run.ID] = inflight
	go func() {
		select {
		case <-inflight.ready:
			timeout <- false
		case <-time.After(time.Duration(2) * time.Second):
			timeout <- true
		}
	}()
}

func (monitor *InflightMonitor) Get(id uint64) *RunContext {
	monitor.Lock()
	defer monitor.Unlock()
	inflight := monitor.mapping[id]
	inflight.ready <- struct{}{}
	return inflight.run
}

func (monitor *InflightMonitor) Remove(id uint64) {
	monitor.Lock()
	defer monitor.Unlock()
	delete(monitor.mapping, id)
}
