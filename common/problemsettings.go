package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	base "github.com/omegaup/go-base"
	"github.com/pkg/errors"
	"math/big"
	"time"
)

// LimitsSettings represents runtime limits for the Input.
type LimitsSettings struct {
	ExtraWallTime        base.Duration
	MemoryLimit          base.Byte
	OutputLimit          base.Byte
	OverallWallTimeLimit base.Duration
	TimeLimit            base.Duration
}

// ValidatorSettings represents the options used to validate outputs.
type ValidatorSettings struct {
	Lang      *string         `json:"Lang,omitempty"`
	Name      string          `json:"Name"`
	Tolerance *float64        `json:"Tolerance,omitempty"`
	Limits    *LimitsSettings `json:"Limits,omitempty"`
}

// InteractiveInterface represents the metadata needed to compile and run
// libinteractive problems.
type InteractiveInterface struct {
	MakefileRules []struct {
		Targets    []string
		Requisites []string
		Compiler   string
		Params     string
		Debug      bool
	}
	ExecutableDescription struct {
		Args []string
		Env  map[string]string
	}
	Files map[string]string
}

// InteractiveSettings contains the information needed by libinteractive to
// generate interactive shims.
type InteractiveSettings struct {
	Interfaces            map[string]map[string]*InteractiveInterface
	Templates             map[string]string
	Main                  string
	ModuleName            string
	ParentLang            string
	LibinteractiveVersion string
}

// CaseSettings contains the information of a single test case.
type CaseSettings struct {
	Name   string
	Weight *big.Rat
}

// MarshalJSON implements the json.Marshaler interface.
func (c *CaseSettings) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name   string
		Weight float64
	}{
		Name:   c.Name,
		Weight: base.RationalToFloat(c.Weight),
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (c *CaseSettings) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	settings := struct {
		Name   string
		Weight float64
	}{}

	if err := json.Unmarshal(data, &settings); err != nil {
		return err
	}

	c.Name = settings.Name
	c.Weight = base.FloatToRational(settings.Weight)

	return nil
}

// A ByCaseName represents a list of CaseSettings associated with a group that
// implements the sort.Interface.
type ByCaseName []CaseSettings

func (c ByCaseName) Len() int           { return len(c) }
func (c ByCaseName) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c ByCaseName) Less(i, j int) bool { return c[i].Name < c[j].Name }

// GroupSettings contains the information of the test case groups.
type GroupSettings struct {
	Cases []CaseSettings
	Name  string
}

// Weight returns the sum of the individual case weights.
func (g *GroupSettings) Weight() *big.Rat {
	weight := &big.Rat{}
	for _, c := range g.Cases {
		weight.Add(weight, c.Weight)
	}
	return weight
}

// A ByGroupName represents a list of GroupSettings associated with a problem
// that implements the sort.Interface.
type ByGroupName []GroupSettings

func (g ByGroupName) Len() int           { return len(g) }
func (g ByGroupName) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g ByGroupName) Less(i, j int) bool { return g[i].Name < g[j].Name }

// ProblemSettings represents the settings of a problem for a particular Input
// set.
type ProblemSettings struct {
	Cases       []GroupSettings      `json:"Cases"`
	Interactive *InteractiveSettings `json:"Interactive,omitempty"`
	Limits      LimitsSettings       `json:"Limits"`
	Slow        bool                 `json:"Slow"`
	Validator   ValidatorSettings    `json:"Validator"`
}

var (
	// DefaultValidatorLimits specifies the default limits for a validator.
	DefaultValidatorLimits = LimitsSettings{
		ExtraWallTime:        base.Duration(0),
		MemoryLimit:          base.Byte(256) * base.Mebibyte,
		OutputLimit:          base.Byte(10) * base.Kibibyte,
		OverallWallTimeLimit: base.Duration(time.Duration(5) * time.Second),
		TimeLimit:            base.Duration(time.Duration(1) * time.Second),
	}

	// DefaultLimits specifies the default limits for a problem.
	DefaultLimits = LimitsSettings{
		ExtraWallTime:        base.Duration(0),
		MemoryLimit:          base.Byte(32) * base.Mebibyte,
		OutputLimit:          base.Byte(10) * base.Kibibyte,
		OverallWallTimeLimit: base.Duration(time.Duration(1) * time.Minute),
		TimeLimit:            base.Duration(time.Duration(1) * time.Second),
	}
)

// ScoreRange represents a minimum and a maximum score.
type ScoreRange struct {
	Min *big.Rat
	Max *big.Rat
}

// MarshalJSON implements the json.Marshaler interface.
func (r *ScoreRange) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(
		"[%f, %f]",
		base.RationalToFloat(r.Min),
		base.RationalToFloat(r.Max),
	)), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (r *ScoreRange) UnmarshalJSON(data []byte) error {
	var floatScores []float64
	if err := json.Unmarshal(data, &floatScores); err != nil {
		return err
	}
	if len(floatScores) != 2 {
		return errors.Errorf("score_range should be an array with two numbers")
	}
	minScore := base.FloatToRational(floatScores[0])
	maxScore := base.FloatToRational(floatScores[1])

	if minScore.Cmp(maxScore) > 0 {
		return errors.Errorf("values for score_range should be sorted")
	}

	if (&big.Rat{}).Cmp(minScore) > 0 || maxScore.Cmp(big.NewRat(1, 1)) > 0 {
		return errors.Errorf("values for score_range should be in the interval [0, 1]")
	}

	r.Min = minScore
	r.Max = maxScore

	return nil
}

// SolutionSettings represents a single testcase with an expected score range
// and/or verdict. At least one of those must be present.
type SolutionSettings struct {
	Filename   string      `json:"filename"`
	ScoreRange *ScoreRange `json:"score_range,omitempty"`
	Verdict    string      `json:"verdict,omitempty"`
	Language   string      `json:"language,omitempty"`
}

// InputsValidatorSettings represents a validator for the .in files.
type InputsValidatorSettings struct {
	Filename string `json:"filename"`
	Language string `json:"language,omitempty"`
}

// TestsSettings represent the tests that are to be run against the problem
// itself. They are stored in tests/settings.json.
type TestsSettings struct {
	Solutions       []SolutionSettings       `json:"solutions"`
	InputsValidator *InputsValidatorSettings `json:"inputs,omitempty"`
}

var (
	// VerdictList is the sorted list of verdicts from worse to better.
	VerdictList = []string{
		"JE",
		"CE",
		"MLE",
		"RFE",
		"RTE",
		"TLE",
		"OLE",
		"WA",
		"PA",
		"AC",
		"OK",
	}
)
