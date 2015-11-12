package common

import (
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	runID uint64
)

func init() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	runID = uint64(r.Int63())
}

type Problem struct {
	Name   string   `json:"name"`
	Points *float64 `json:"points,omitempty"`
}

type Run struct {
	ID        uint64  `json:"id"`
	GUID      string  `json:"guid"`
	Contest   *string `json:"contest,omitempty"`
	Language  string  `json:"language"`
	InputHash string  `json:"input_hash"`
	Problem   Problem `json:"problem"`
	Source    string  `json:"source"`
}

func NewRunID() uint64 {
	return atomic.AddUint64(&runID, 1)
}

func (run *Run) UpdateID() uint64 {
	run.ID = NewRunID()
	return run.ID
}
