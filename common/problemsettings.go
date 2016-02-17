package common

// ValidatorSettings represents the options used to validate outputs.
type ValidatorSettings struct {
	Lang      *string  `json:"Lang,omitempty"`
	Name      string   `json:"Name"`
	Tolerance *float64 `json:"Tolerance,omitempty"`
}

// LimitsSettings represents runtime limits for the Input.
type LimitsSettings struct {
	ExtraWallTime        int64
	MemoryLimit          int64
	OutputLimit          int64
	OverallWallTimeLimit int64
	StackLimit           int64
	TimeLimit            int64
	ValidatorTimeLimit   int64
}

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

type ByCaseName []CaseSettings

func (c ByCaseName) Len() int           { return len(c) }
func (c ByCaseName) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c ByCaseName) Less(i, j int) bool { return c[i].Name < c[j].Name }

// GroupSettings contains the information of the test case groups.
type GroupSettings struct {
	Cases  []CaseSettings
	Name   string
	Weight float64
}

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
