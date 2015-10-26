package queue

import (
	"github.com/omegaup/quark/common"
	"reflect"
)

type Queue struct {
	Name  string
	runs  [3]chan *common.Run
	ready chan struct{}
}

func NewQueue(name string, channelLength int) *Queue {
	queue := &Queue{
		Name:  name,
		ready: make(chan struct{}, channelLength),
	}
	for r := range queue.runs {
		queue.runs[r] = make(chan *common.Run, channelLength)
	}
	return queue
}

// TryGetRun goes through the channels in order of priority, and if one of them
// has something ready, it returns it. This behaves more or less like
//     run, ok := <-queue
func (queue *Queue) TryGetRun() (*common.Run, bool) {
	for i := range queue.runs {
		if run, ok := <-queue.runs[i]; ok {
			return run, ok
		}
	}
	return nil, false
}

// GetRun
func (queue *Queue) GetRun(output chan<- *common.Run) {
	if run, ok := queue.TryGetRun(); ok {
		output <- run
		return
	}
	// All channels were empty. Wait for the first one that has something to
	// continue.
	select {
	case run := <-queue.runs[0]:
		output <- run
	case run := <-queue.runs[1]:
		output <- run
	case run := <-queue.runs[2]:
		output <- run
	}
}

// Close closes all of the Queue's run queues and drains them into output.
func (queue *Queue) Close(output chan<- *common.Run) {
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

func (pool *Pool) GetRun(output chan<- *common.Run) {
	for i := range pool.queues {
		if _, ok := <-pool.queues[i].ready; ok {
			pool.queues[i].GetRun(output)
			return
		}
	}

	// Otherwise wait until one of them has something.
	chosen, _, _ := reflect.Select(pool.cases)
	pool.queues[chosen].GetRun(output)
}
