package common

// ValidatorSettings represents the options used to validate outputs.
type ValidatorSettings struct {
	Name      string
	Lang      string
	Tolerance *float64
}

// LimitsSettings represents runtime limits for the Input.
type LimitsSettings struct {
	TimeLimit            int
	StackLimit           int
	MemoryLimit          int
	OverallWallTimeLimit int
	ExtraWallTime        int
	OutputLimit          int
	ValidatorTimeLimit   int
}

// InteractiveSettings contains the information needed by libinteractive to
// generate interactive shims.
type InteractiveSettings struct {
	Lang      string
	Interface string
}

// CaseSettings contains the information of a single test case.
type CaseSettings struct {
	Name   string
	Weight float64
}

// GroupSettings contains the information of the test case groups.
type GroupSettings struct {
	Name   string
	Weight float64
	Cases  []CaseSettings
}

// ProblemSettings represents the settings of a problem for a particular Input
// set.
type ProblemSettings struct {
	Validator   ValidatorSettings
	Slow        bool
	Limits      LimitsSettings
	Cases       []GroupSettings
	Interactive *InteractiveSettings
}
