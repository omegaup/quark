package runner

import (
	"fmt"
	"github.com/omegaup/quark/common"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

type CaseResult struct {
	Name     string
	MaxScore float64
	Score    float64
	Verdict  string
	Meta     RunMetadata
}

type GroupResult struct {
	Group    string
	MaxScore float64
	Score    float64
	Cases    []CaseResult
}

type RunResult struct {
	Verdict      string
	CompileError *string
	Score        float64
	Time         float64
	WallTime     float64
	Memory       int
	Groups       []GroupResult
}

func Grade(ctx *common.Context, run *common.Run,
	input common.Input) (*RunResult, error) {
	runResult := &RunResult{
		Verdict: "JE",
	}
	runRoot := path.Join(ctx.Config.Runner.RuntimePath, "grade",
		strconv.FormatUint(run.ID, 10))
	binPath := path.Join(runRoot, "bin")
	if err := os.MkdirAll(binPath, 0755); err != nil {
		return runResult, err
	}
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(runRoot)
	}

	mainFile := path.Join(binPath, "Main."+run.Language)
	err := ioutil.WriteFile(mainFile, []byte(run.Source), 0644)
	if err != nil {
		return runResult, err
	}

	ctx.Log.Info("Running", "run", run)

	compileMeta, err := Compile(ctx, run.Language, []string{mainFile}, binPath,
		path.Join(runRoot, "compile.out"), path.Join(runRoot, "compile.err"),
		path.Join(runRoot, "compile.meta"), "Main", []string{})

	if err != nil {
		ctx.Log.Error("Compile error", "err", err, "compileMeta", compileMeta)
		runResult.Verdict = "CE"
		return runResult, err
	}

	rawResults := make(map[string]*RunMetadata)
	runResult.Verdict = "OK"
	wallTimeLimit := (float64)(input.Settings().Limits.OverallWallTimeLimit / 1000.0)
	// TODO(lhchavez): Provide a stable ordering of cases.
	for _, group := range input.Settings().Cases {
		for _, caseData := range group.Cases {
			var runMeta *RunMetadata
			if runResult.WallTime > wallTimeLimit {
				runMeta = &RunMetadata{
					Verdict: "TLE",
				}
			} else {
				runMeta, err = Run(ctx, input,
					run.Language, binPath,
					path.Join(input.Path(), "in", fmt.Sprintf("%s.in", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.out", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.err", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.meta", caseData.Name)),
					"Main", nil, nil, nil, []string{}, map[string]string{})
				if err != nil {
					ctx.Log.Error("failed to run "+caseData.Name, "err", err)
				}
			}
			rawResults[caseData.Name] = runMeta
			runResult.Verdict = worseVerdict(runResult.Verdict, runMeta.Verdict)
			runResult.Time += runMeta.Time
			runResult.WallTime += runMeta.WallTime
			runResult.Memory = max(runResult.Memory, runMeta.Memory)
		}
	}

	// Validate outputs.
	maxScore := 1.0
	if run.Problem.Points != nil {
		maxScore = *run.Problem.Points
	}
	groupResults := make([]GroupResult, len(input.Settings().Cases))
	for i, group := range input.Settings().Cases {
		caseResults := make([]CaseResult, len(group.Cases))
		correct := true
		score := 0.0
		for j, caseData := range group.Cases {
			caseResults[j] = CaseResult{
				Name:     caseData.Name,
				MaxScore: maxScore * caseData.Weight,
				Verdict:  rawResults[caseData.Name].Verdict,
				Meta:     *rawResults[caseData.Name],
			}
			if caseResults[j].Verdict == "OK" {
				runScore := CalculateScore(ctx, input.Settings(), caseData,
					path.Join(input.Path(), "out", fmt.Sprintf("%s.out", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.out", caseData.Name)))
				caseResults[j].Score = maxScore * runScore * caseData.Weight
				score += runScore * caseData.Weight
				if runScore == 0 {
					correct = false
				}
				if runScore != 1 {
					runResult.Verdict = worseVerdict(runResult.Verdict, "PA")
				}
			}
		}
		if !correct {
			score = 0
		}
		groupResults[i] = GroupResult{
			Group:    group.Name,
			MaxScore: maxScore * group.Weight,
			Score:    maxScore * score,
		}
		runResult.Score += groupResults[i].Score
	}

	runResult.Groups = groupResults

	if runResult.Verdict == "PA" && runResult.Score == 0 {
		runResult.Verdict = "WA"
	}

	ctx.Log.Debug("Finished running", "results", runResult)
	return runResult, nil
}

func worseVerdict(a, b string) string {
	verdictList := []string{
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
	idxA := sliceIndex(len(verdictList),
		func(i int) bool { return verdictList[i] == a })
	idxB := sliceIndex(len(verdictList),
		func(i int) bool { return verdictList[i] == b })
	return verdictList[min(idxA, idxB)]
}

func sliceIndex(limit int, predicate func(i int) bool) int {
	for i := 0; i < limit; i++ {
		if predicate(i) {
			return i
		}
	}
	return -1
}
