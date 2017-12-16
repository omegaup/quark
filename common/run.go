package common

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	attemptID uint64
)

func init() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	attemptID = uint64(r.Int63())
}

// A Run represents an omegaUp run.
type Run struct {
	AttemptID uint64  `json:"attempt_id"`
	Source    string  `json:"source"`
	Language  string  `json:"language"`
	InputHash string  `json:"input_hash"`
	MaxScore  float64 `json:"max_score"`
	Debug     bool    `json:"debug"`
}

// NewAttemptID allocates a locally-unique AttemptID. A counter is initialized
// to a random 63-bit integer on startup and then atomically incremented eacn
// time a new ID is needed.
func NewAttemptID() uint64 {
	return atomic.AddUint64(&attemptID, 1)
}

// UpdateAttemptID assigns a new AttemptID to a run.
func (run *Run) UpdateAttemptID() uint64 {
	run.AttemptID = NewAttemptID()
	return run.AttemptID
}

func (run *Run) String() string {
	return fmt.Sprintf(
		"Run{AttemptID:%d Language:%s InputHash:%s}",
		run.AttemptID,
		run.Language,
		run.InputHash,
	)
}
