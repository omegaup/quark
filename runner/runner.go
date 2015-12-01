package runner

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
)

type CaseResult struct {
	Verdict  string                 `json:"verdict"`
	Name     string                 `json:"name"`
	MaxScore float64                `json:"max_score"`
	Score    float64                `json:"score"`
	Meta     map[string]RunMetadata `json:"meta"`
}

type GroupResult struct {
	Group    string       `json:"group"`
	MaxScore float64      `json:"max_score"`
	Score    float64      `json:"score"`
	Cases    []CaseResult `json:"cases"`
}

type RunResult struct {
	Verdict      string                 `json:"verdict"`
	CompileError *string                `json:"compile_error,omitempty"`
	CompileMeta  map[string]RunMetadata `json:"compile_meta"`
	Score        float64                `json:"score"`
	Time         float64                `json:"time"`
	WallTime     float64                `json:"wall_time"`
	Memory       int                    `json:"memory"`
	Groups       []GroupResult          `json:"groups"`
}

func Grade(
	ctx *common.Context,
	client *http.Client,
	baseURL *url.URL,
	run *common.Run,
	input common.Input,
	sandbox Sandbox,
) (*RunResult, error) {
	runResult := &RunResult{
		Verdict: "JE",
	}
	if !sandbox.Supported() {
		return runResult, errors.New("Sandbox not supported")
	}
	runRoot := path.Join(
		ctx.Config.Runner.RuntimePath,
		"grade",
		strconv.FormatUint(run.ID, 10),
	)
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

	compileMeta, err := sandbox.Compile(
		ctx,
		run.Language,
		[]string{mainFile},
		binPath,
		path.Join(runRoot, "compile.out"),
		path.Join(runRoot, "compile.err"),
		path.Join(runRoot, "compile.meta"),
		"Main",
		[]string{},
	)

	if compileMeta != nil {
		runResult.CompileMeta = map[string]RunMetadata{
			"Main": *compileMeta,
		}
	}

	if err != nil || compileMeta.Verdict != "OK" {
		ctx.Log.Error("Compile error", "err", err, "compileMeta", compileMeta)
		runResult.Verdict = "CE"
		compileError := getCompileError(path.Join(runRoot, "compile.err"))
		runResult.CompileError = &compileError
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
				runMeta, err = sandbox.Run(
					ctx,
					input,
					run.Language,
					binPath,
					path.Join(input.Path(), "in", fmt.Sprintf("%s.in", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.out", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.err", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.meta", caseData.Name)),
					"Main",
					nil,
					nil,
					nil,
					[]string{},
					map[string]string{},
				)
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
				Meta: map[string]RunMetadata{
					"Main": *rawResults[caseData.Name],
				},
			}
			if caseResults[j].Verdict == "OK" {
				contestantPath := path.Join(
					runRoot, fmt.Sprintf("%s.out", caseData.Name),
				)
				contestantFd, err := os.Open(contestantPath)
				if err != nil {
					ctx.Log.Warn("Error opening file", "path", contestantPath, "err", err)
					continue
				}
				defer contestantFd.Close()
				expectedPath := path.Join(
					input.Path(), "out", fmt.Sprintf("%s.out", caseData.Name),
				)
				expectedFd, err := os.Open(expectedPath)
				if err != nil {
					ctx.Log.Warn("Error opening file", "path", expectedPath, "err", err)
					continue
				}
				defer expectedFd.Close()
				runScore, err := CalculateScore(
					&input.Settings().Validator,
					contestantFd,
					expectedFd,
				)
				if err != nil {
					ctx.Log.Debug("error comparing values", "err", err)
				}
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
			Cases:    caseResults,
		}
		runResult.Score += groupResults[i].Score
	}

	runResult.Groups = groupResults

	if runResult.Verdict == "PA" && runResult.Score == 0 {
		runResult.Verdict = "WA"
	} else if runResult.Verdict == "OK" {
		runResult.Verdict = "AC"
	}

	ctx.Log.Debug("Finished running", "results", runResult)
	filesURL, err := baseURL.Parse(fmt.Sprintf("run/%d/files/", run.ID))
	if err != nil {
		return runResult, err
	}
	if err := uploadFiles(
		ctx,
		client,
		filesURL.String(),
		runRoot,
		input,
	); err != nil {
		return runResult, err
	}

	return runResult, nil
}

func uploadFiles(
	ctx *common.Context,
	client *http.Client,
	uploadURL string,
	runRoot string,
	input common.Input,
) error {
	files := []string{
		"compile.out",
		"compile.err",
		"compile.meta",
	}
	for _, group := range input.Settings().Cases {
		for _, caseData := range group.Cases {
			files = append(files,
				fmt.Sprintf("%s.out", caseData.Name),
				fmt.Sprintf("%s.err", caseData.Name),
				fmt.Sprintf("%s.meta", caseData.Name),
			)
		}
	}

	path, err := createZipFile(runRoot, files)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	fd, err := os.Open(path)
	if err != nil {
		return err
	}

	resp, err := client.Post(uploadURL, "application/zip", fd)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func createZipFile(runRoot string, files []string) (string, error) {
	zipFd, err := ioutil.TempFile(runRoot, ".results_zip")
	if err != nil {
		return "", err
	}
	zipPath := zipFd.Name()
	defer zipFd.Close()
	zip := zip.NewWriter(zipFd)
	for _, file := range files {
		f, err := os.Open(path.Join(runRoot, file))
		if err != nil {
			continue
		}
		defer f.Close()
		zf, err := zip.Create(file)
		if err != nil {
			return zipPath, err
		}
		if _, err := io.Copy(zf, f); err != nil {
			return zipPath, err
		}
	}
	return zipPath, zip.Close()
}

func getCompileError(errorFile string) string {
	fd, err := os.Open(errorFile)
	if err != nil {
		return err.Error()
	}
	defer fd.Close()
	bytes, err := ioutil.ReadAll(fd)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
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
