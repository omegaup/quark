package runner

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"syscall"
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
	MaxScore     float64                `json:"max_score"`
	Time         float64                `json:"time"`
	WallTime     float64                `json:"wall_time"`
	Memory       int                    `json:"memory"`
	Groups       []GroupResult          `json:"groups"`
}

type binaryType int

const (
	binaryProblemsetter binaryType = iota
	binaryContestant
	binaryValidator
)

type binary struct {
	name             string
	language         string
	binPath          string
	outputPathPrefix string
	binaryType       binaryType
	receiveInput     bool
	sourceFiles      []string
	extraFlags       []string
	extraMountPoints map[string]string
}

type intermediateRunResult struct {
	runMeta    *RunMetadata
	binaryType binaryType
}

func extraParentFlags(language string) []string {
	if language == "c" || language == "cpp" || language == "cpp11" {
		return []string{"-Wl,-e__entry"}
	}
	return []string{}
}

func normalizedSourceFiles(
	runRoot string,
	name string,
	iface *common.InteractiveInterface,
) []string {
	binRoot := path.Join(runRoot, name, "bin")
	sources := make([]string, len(iface.MakefileRules[0].Requisites))
	for idx, requisite := range iface.MakefileRules[0].Requisites {
		sources[idx] = path.Join(binRoot, path.Base(requisite))
	}
	return sources
}

func generateParentMountpoints(
	runRoot string,
	interactive *common.InteractiveSettings,
) map[string]string {
	result := make(map[string]string)
	for name, _ := range interactive.Interfaces {
		if name == interactive.Main {
			continue
		}
		for src, dst := range generateMountpoint(runRoot, name) {
			result[src] = dst
		}
	}
	return result
}

func generateMountpoint(
	runRoot string,
	name string,
) map[string]string {
	return map[string]string{
		path.Join(runRoot, name, "pipes"): fmt.Sprintf("/home/%s_pipes", name),
	}
}

func Grade(
	ctx *common.Context,
	filesWriter io.Writer,
	run *common.Run,
	input common.Input,
	sandbox Sandbox,
) (*RunResult, error) {
	runResult := &RunResult{
		Verdict:  "JE",
		MaxScore: run.MaxScore,
	}
	if !sandbox.Supported() {
		return runResult, errors.New("Sandbox not supported")
	}
	runRoot := path.Join(
		ctx.Config.Runner.RuntimePath,
		"grade",
		strconv.FormatUint(run.AttemptID, 10),
	)
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(runRoot)
	}

	ctx.Log.Info("Running", "run", run)

	var binaries []*binary

	interactive := input.Settings().Interactive
	if interactive != nil {
		ctx.Log.Info("libinteractive", "version", interactive.LibinteractiveVersion)
		binaries = []*binary{
			&binary{
				interactive.Main,
				interactive.ParentLang,
				path.Join(runRoot, interactive.Main, "bin"),
				"",
				binaryProblemsetter,
				true,
				normalizedSourceFiles(
					runRoot,
					interactive.Main,
					interactive.Interfaces[interactive.Main][interactive.ParentLang],
				),
				extraParentFlags(interactive.ParentLang),
				generateParentMountpoints(runRoot, interactive),
			},
		}
		for name, lang_iface := range interactive.Interfaces {
			if name == interactive.Main {
				continue
			}
			binaries = append(
				binaries,
				&binary{
					name,
					run.Language,
					path.Join(runRoot, name, "bin"),
					name,
					binaryContestant,
					false,
					normalizedSourceFiles(
						runRoot,
						name,
						lang_iface[run.Language],
					),
					[]string{},
					generateMountpoint(runRoot, name),
				},
			)
		}

		// Setup all source files.
		for _, bin := range binaries {
			binPath := path.Join(runRoot, bin.name, "bin")
			if err := os.MkdirAll(binPath, 0755); err != nil {
				return runResult, err
			}
		}
		if err := os.Link(
			path.Join(
				input.Path(),
				fmt.Sprintf("interactive/Main.%s", interactive.ParentLang),
			),
			path.Join(
				runRoot,
				fmt.Sprintf("Main/bin/Main.%s", interactive.ParentLang),
			),
		); err != nil {
			return runResult, err
		}
		for name, lang_iface := range interactive.Interfaces {
			var lang string
			if name == "Main" {
				lang = interactive.ParentLang
			} else {
				lang = run.Language
			}
			for filename, contents := range lang_iface[lang].Files {
				sourcePath := path.Join(
					runRoot,
					fmt.Sprintf("%s/bin/%s", name, path.Base(filename)),
				)
				err := ioutil.WriteFile(sourcePath, []byte(contents), 0644)
				if err != nil {
					return runResult, err
				}
			}
			if name == "Main" {
				for iface_name, _ := range interactive.Interfaces {
					if iface_name == "Main" {
						continue
					}
					pipesMountPath := path.Join(
						runRoot,
						fmt.Sprintf("%s/bin/%s_pipes", name, iface_name),
					)
					if err := os.MkdirAll(pipesMountPath, 0755); err != nil {
						return runResult, err
					}
				}
				continue
			}
			sourcePath := path.Join(
				runRoot,
				fmt.Sprintf("%s/bin/%s.%s", name, interactive.ModuleName, run.Language),
			)
			err := ioutil.WriteFile(sourcePath, []byte(run.Source), 0644)
			if err != nil {
				return runResult, err
			}
			pipesMountPath := path.Join(
				runRoot,
				fmt.Sprintf("%s/bin/%s_pipes", name, name),
			)
			if err := os.MkdirAll(pipesMountPath, 0755); err != nil {
				return runResult, err
			}
			pipesPath := path.Join(runRoot, name, "pipes")
			if err := os.MkdirAll(pipesPath, 0755); err != nil {
				return runResult, err
			}
			if err := syscall.Mkfifo(path.Join(pipesPath, "in"), 0644); err != nil {
				return runResult, err
			}
			if err := syscall.Mkfifo(path.Join(pipesPath, "out"), 0644); err != nil {
				return runResult, err
			}
		}
	} else {
		// Setup all source files.
		mainBinPath := path.Join(runRoot, "Main", "bin")
		if err := os.MkdirAll(mainBinPath, 0755); err != nil {
			return runResult, err
		}
		mainSourcePath := path.Join(mainBinPath, fmt.Sprintf("Main.%s", run.Language))
		err := ioutil.WriteFile(mainSourcePath, []byte(run.Source), 0644)
		if err != nil {
			return runResult, err
		}

		binaries = []*binary{
			&binary{
				"Main",
				run.Language,
				mainBinPath,
				"",
				binaryContestant,
				true,
				[]string{mainSourcePath},
				[]string{},
				map[string]string{},
			},
		}
	}

	generatedFiles := make([]string, 0)

	validatorBinPath := path.Join(runRoot, "validator", "bin")
	regularBinaryCount := len(binaries)
	if input.Settings().Validator.Name == "custom" {
		if err := os.MkdirAll(validatorBinPath, 0755); err != nil {
			return runResult, err
		}
		validatorLang := *input.Settings().Validator.Lang
		validatorFileName := fmt.Sprintf("validator.%s", validatorLang)
		validatorSourceFile := path.Join(validatorBinPath, validatorFileName)
		err := os.Link(path.Join(input.Path(), validatorFileName), validatorSourceFile)
		if err != nil {
			return runResult, err
		}
		binaries = append(
			binaries,
			&binary{
				"validator",
				validatorLang,
				validatorBinPath,
				"validator",
				binaryValidator,
				false,
				[]string{},
				[]string{},
				map[string]string{},
			},
		)
	}

	runResult.CompileMeta = make(map[string]RunMetadata)

	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("compile", common.EventBegin))
	for _, b := range binaries {
		binRoot := path.Join(runRoot, b.name)
		binPath := path.Join(binRoot, "bin")

		singleCompileEvent := ctx.EventFactory.NewCompleteEvent(
			b.name,
			common.Arg{"language", b.language},
		)
		compileMeta, err := sandbox.Compile(
			ctx,
			b.language,
			b.sourceFiles,
			binPath,
			path.Join(binRoot, "compile.out"),
			path.Join(binRoot, "compile.err"),
			path.Join(binRoot, "compile.meta"),
			b.name,
			b.extraFlags,
		)
		ctx.EventCollector.Add(singleCompileEvent)
		generatedFiles = append(
			generatedFiles,
			path.Join(b.name, "compile.out"),
			path.Join(b.name, "compile.err"),
			path.Join(b.name, "compile.meta"),
		)

		if compileMeta != nil {
			runResult.CompileMeta[b.name] = *compileMeta
		}

		if err != nil || compileMeta.Verdict != "OK" {
			ctx.Log.Error("Compile error", "err", err, "compileMeta", compileMeta)
			runResult.Verdict = "CE"
			compileErrorFile := "compile.err"
			if b.language == "pas" {
				// Lazarus writes the output of the compile error in compile.out.
				compileErrorFile = "compile.out"
			} else {
				compileErrorFile = "compile.err"
			}
			compileError := getCompileError(path.Join(binRoot, compileErrorFile))
			runResult.CompileError = &compileError
			ctx.EventCollector.Add(ctx.EventFactory.NewEvent("compile", common.EventEnd))
			return runResult, err
		}
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("compile", common.EventEnd))

	groupResults := make([]GroupResult, len(input.Settings().Cases))
	runResult.Verdict = "OK"
	wallTimeLimit := (float64)(input.Settings().Limits.OverallWallTimeLimit / 1000.0)
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("run", common.EventBegin))
	for i, group := range input.Settings().Cases {
		caseResults := make([]CaseResult, len(group.Cases))
		for j, caseData := range group.Cases {
			var runMeta *RunMetadata
			if runResult.WallTime > wallTimeLimit {
				runMeta = &RunMetadata{
					Verdict: "TLE",
				}
			} else {
				singleRunEvent := ctx.EventFactory.NewCompleteEvent(caseData.Name)
				metaChan := make(chan intermediateRunResult, 1)
				for _, bin := range binaries {
					if bin.binaryType == binaryValidator {
						continue
					}
					go func(bin *binary) {
						var inputPath string
						if bin.receiveInput {
							inputPath = path.Join(
								input.Path(),
								"in",
								fmt.Sprintf("%s.in", caseData.Name),
							)
						} else {
							inputPath = "/dev/null"
						}
						runMeta, err := sandbox.Run(
							ctx,
							input,
							bin.language,
							bin.binPath,
							inputPath,
							path.Join(
								runRoot,
								bin.outputPathPrefix,
								fmt.Sprintf("%s.out", caseData.Name),
							),
							path.Join(
								runRoot,
								bin.outputPathPrefix,
								fmt.Sprintf("%s.err", caseData.Name),
							),
							path.Join(
								runRoot,
								bin.outputPathPrefix,
								fmt.Sprintf("%s.meta", caseData.Name),
							),
							bin.name,
							nil,
							nil,
							nil,
							[]string{},
							bin.extraMountPoints,
						)
						if err != nil {
							ctx.Log.Error(
								"failed to run "+caseData.Name,
								"interface", bin.name,
								"err", err,
							)
						}
						generatedFiles = append(
							generatedFiles,
							path.Join(
								bin.outputPathPrefix,
								fmt.Sprintf("%s.out", caseData.Name),
							),
							path.Join(
								bin.outputPathPrefix,
								fmt.Sprintf("%s.err", caseData.Name),
							),
							path.Join(
								bin.outputPathPrefix,
								fmt.Sprintf("%s.meta", caseData.Name),
							),
						)
						metaChan <- intermediateRunResult{runMeta, bin.binaryType}
					}(bin)
				}
				var parentMetadata *RunMetadata = nil
				chosenMetadata := RunMetadata{
					Verdict: "OK",
				}
				chosenMetadataEmpty := true
				var totalTime float64 = 0
				var totalWallTime float64 = 0
				totalMemory := 0
				for i := 0; i < regularBinaryCount; i++ {
					intermediateResult := <-metaChan
					if intermediateResult.binaryType == binaryProblemsetter {
						parentMetadata = intermediateResult.runMeta
					} else {
						if intermediateResult.runMeta.Verdict != "OK" {
							if chosenMetadataEmpty {
								chosenMetadata = *intermediateResult.runMeta
								chosenMetadataEmpty = false
							}
						}
						totalTime += intermediateResult.runMeta.Time
						totalWallTime += intermediateResult.runMeta.WallTime
						totalMemory += max(totalMemory, intermediateResult.runMeta.Memory)
					}
				}
				close(metaChan)
				ctx.EventCollector.Add(singleRunEvent)
				chosenMetadata.Time = totalTime
				chosenMetadata.WallTime = totalWallTime
				chosenMetadata.Memory = totalMemory

				if parentMetadata != nil && parentMetadata.Verdict != "OK" {
					// TODO: https://github.com/omegaup/backend/blob/master/runner/src/main/scala/com/omegaup/runner/Runner.scalaL582
				}

				runMeta = &chosenMetadata
			}
			runResult.Verdict = worseVerdict(runResult.Verdict, runMeta.Verdict)
			runResult.Time += runMeta.Time
			runResult.WallTime += runMeta.WallTime
			runResult.Memory = max(runResult.Memory, runMeta.Memory)

			// TODO: change CaseResult to split original metadatas and final metadata
			caseResults[j] = CaseResult{
				Name:     caseData.Name,
				MaxScore: runResult.MaxScore * caseData.Weight,
				Verdict:  runMeta.Verdict,
				Meta: map[string]RunMetadata{
					"Main": *runMeta,
				},
			}
		}
		groupResults[i] = GroupResult{
			Group:    group.Name,
			MaxScore: runResult.MaxScore * group.Weight,
			Score:    0,
			Cases:    caseResults,
		}
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("run", common.EventEnd))

	// Validate outputs.
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("validate", common.EventBegin))
	for i, group := range input.Settings().Cases {
		correct := true
		score := 0.0
		for j, caseData := range group.Cases {
			caseResults := groupResults[i].Cases[j]
			if caseResults.Verdict == "OK" {
				contestantPath := path.Join(
					runRoot, fmt.Sprintf("%s.out", caseData.Name),
				)
				if input.Settings().Validator.Name == "custom" {
					originalInputFile := path.Join(
						input.Path(),
						"in",
						fmt.Sprintf("%s.in", caseData.Name),
					)
					originalOutputFile := path.Join(
						input.Path(),
						"out",
						fmt.Sprintf("%s.out", caseData.Name),
					)
					runMetaFile := path.Join(runRoot, fmt.Sprintf("%s.meta", caseData.Name))
					validateMeta, err := sandbox.Run(
						ctx,
						input,
						*input.Settings().Validator.Lang,
						validatorBinPath,
						contestantPath,
						path.Join(runRoot, "validator", fmt.Sprintf("%s.out", caseData.Name)),
						path.Join(runRoot, "validator", fmt.Sprintf("%s.err", caseData.Name)),
						path.Join(runRoot, "validator", fmt.Sprintf("%s.meta", caseData.Name)),
						"validator",
						&originalInputFile,
						&originalOutputFile,
						&runMetaFile,
						[]string{},
						map[string]string{},
					)
					if err != nil {
						ctx.Log.Error("failed to validate "+caseData.Name, "err", err)
					}
					generatedFiles = append(
						generatedFiles,
						fmt.Sprintf("validator/%s.out", caseData.Name),
						fmt.Sprintf("validator/%s.err", caseData.Name),
						fmt.Sprintf("validator/%s.meta", caseData.Name),
					)
					if validateMeta.Verdict != "OK" {
						// If the validator did not exit cleanly, assume an empty output.
						contestantPath = "/dev/null"
					} else {
						contestantPath = path.Join(
							runRoot,
							"validator",
							fmt.Sprintf("%s.out", caseData.Name),
						)
					}
				}
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
				caseResults.Score = runResult.MaxScore * runScore * caseData.Weight
				score += runScore * caseData.Weight
				if runScore == 0 {
					correct = false
				}
				if runScore != 1 {
					runResult.Verdict = worseVerdict(runResult.Verdict, "PA")
				}
			}
		}
		if correct {
			groupResults[i].Score = runResult.MaxScore * score
			runResult.Score += groupResults[i].Score
		}
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("validate", common.EventEnd))

	runResult.Groups = groupResults

	if runResult.Verdict == "PA" && runResult.Score == 0 {
		runResult.Verdict = "WA"
	} else if runResult.Verdict == "OK" {
		runResult.Verdict = "AC"
		runResult.Score = runResult.MaxScore
	}

	ctx.Log.Debug(
		"Finished running",
		"id", run.AttemptID,
		"verdict", runResult.Verdict,
		"score", runResult.Score,
	)
	uploadEvent := ctx.EventFactory.NewCompleteEvent("upload")
	defer ctx.EventCollector.Add(uploadEvent)
	if err := uploadFiles(
		ctx,
		filesWriter,
		runRoot,
		input,
		generatedFiles,
	); err != nil {
		ctx.Log.Debug("uploadFiles failed", "err", err)
		return runResult, err
	}

	return runResult, nil
}

func uploadFiles(
	ctx *common.Context,
	filesWriter io.Writer,
	runRoot string,
	input common.Input,
	files []string,
) error {
	path, err := createZipFile(runRoot, files)
	if path != "" {
		defer os.Remove(path)
	}
	if err != nil {
		return err
	}
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()
	_, err = io.Copy(filesWriter, fd)
	return err
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
