package common

import (
	"time"
)

// LimitsSettings represents runtime limits for the Input.
type LimitsSettings struct {
	ExtraWallTime        Duration
	MemoryLimit          Byte
	OutputLimit          Byte
	OverallWallTimeLimit Duration
	TimeLimit            Duration
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
	Main                  string
	ModuleName            string
	ParentLang            string
	LibinteractiveVersion string
}

// CaseSettings contains the information of a single test case.
type CaseSettings struct {
	Name   string
	Weight float64
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
func (g *GroupSettings) Weight() float64 {
	weight := 0.0
	for _, c := range g.Cases {
		weight += c.Weight
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
		ExtraWallTime:        Duration(0),
		MemoryLimit:          Byte(256) * Mebibyte,
		OutputLimit:          Byte(10) * Kibibyte,
		OverallWallTimeLimit: Duration(time.Duration(5) * time.Second),
		TimeLimit:            Duration(time.Duration(1) * time.Second),
	}

	// DefaultLimits specifies the default limits for a problem.
	DefaultLimits = LimitsSettings{
		ExtraWallTime:        Duration(0),
		MemoryLimit:          Byte(32) * Mebibyte,
		OutputLimit:          Byte(10) * Kibibyte,
		OverallWallTimeLimit: Duration(time.Duration(1) * time.Minute),
		TimeLimit:            Duration(time.Duration(1) * time.Second),
	}
)
