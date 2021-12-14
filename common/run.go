package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	base "github.com/omegaup/go-base/v3"
	"math/big"
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
	AttemptID   uint64   `json:"attempt_id"`
	Source      string   `json:"source"`
	Language    string   `json:"language"`
	ProblemName string   `json:"problem"`
	Commit      string   `json:"commit"`
	InputHash   string   `json:"input_hash"`
	MaxScore    *big.Rat `json:"max_score"`
	Debug       bool     `json:"debug"`
}

// MarshalJSON implements the json.Marshaler interface.
func (r *Run) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		AttemptID   uint64  `json:"attempt_id"`
		Source      string  `json:"source"`
		Language    string  `json:"language"`
		ProblemName string  `json:"problem"`
		InputHash   string  `json:"input_hash"`
		MaxScore    float64 `json:"max_score"`
		Debug       bool    `json:"debug"`
	}{
		AttemptID:   r.AttemptID,
		Source:      r.Source,
		Language:    r.Language,
		ProblemName: r.ProblemName,
		InputHash:   r.InputHash,
		MaxScore:    base.RationalToFloat(r.MaxScore),
		Debug:       r.Debug,
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (r *Run) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	run := struct {
		AttemptID   uint64  `json:"attempt_id"`
		Source      string  `json:"source"`
		Language    string  `json:"language"`
		ProblemName string  `json:"problem"`
		InputHash   string  `json:"input_hash"`
		MaxScore    float64 `json:"max_score"`
		Debug       bool    `json:"debug"`
	}{}

	if err := json.Unmarshal(data, &run); err != nil {
		return err
	}

	r.AttemptID = run.AttemptID
	r.Source = run.Source
	r.Language = run.Language
	r.ProblemName = run.ProblemName
	r.InputHash = run.InputHash
	r.MaxScore = base.FloatToRational(run.MaxScore)
	r.Debug = run.Debug

	return nil
}

// NewAttemptID allocates a locally-unique AttemptID. A counter is initialized
// to a random 63-bit integer on startup and then atomically incremented eacn
// time a new ID is needed.
func NewAttemptID() uint64 {
	return atomic.AddUint64(&attemptID, 1)
}

// UpdateAttemptID assigns a new AttemptID to a run.
func (r *Run) UpdateAttemptID() uint64 {
	r.AttemptID = NewAttemptID()
	return r.AttemptID
}

func (r *Run) String() string {
	return fmt.Sprintf(
		"Run{AttemptID:%d Language:%s ProblemName:%s InputHash:%s}",
		r.AttemptID,
		r.Language,
		r.ProblemName,
		r.InputHash,
	)
}
