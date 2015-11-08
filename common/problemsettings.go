package common

type ValidatorSettings struct {
	Name string
	Lang string
}

type LimitsSettings struct {
	TimeLimit            int
	StackLimit           int
	MemoryLimit          int
	OverallWallTimeLimit int
	ExtraWallTime        int
	OutputLimit          int
	ValidatorTimeLimit   int
}

type InteractiveSettings struct {
	Lang      string
	Interface string
}

type CaseSettings struct {
	Name   string
	Weight float64
}

type GroupSettings struct {
	Name   string
	Weight float64
	Cases  []CaseSettings
}

type ProblemSettings struct {
	Validator   ValidatorSettings
	Slow        bool
	Limits      LimitsSettings
	Cases       []GroupSettings
	Interactive *InteractiveSettings
}
