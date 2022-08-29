package ci

import (
	"bytes"
	"compress/gzip"
	"encoding"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/go-base/v3/logging"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"

	"github.com/pkg/errors"
)

const (
	// CopyStdinToStdoutSource is a small C++ program that just copies stdin to
	// stdout.
	CopyStdinToStdoutSource = `
		#include <stdint.h>
		#include <stdio.h>
		#include <unistd.h>

		int main() {
			uint8_t buffer[4096];
			while (true) {
				ssize_t read_bytes = read(STDIN_FILENO, buffer, sizeof(buffer));
				if (read_bytes < 0) {
					perror("read");
					return 1;
				}
				if (read_bytes == 0) {
					break;
				}
				uint8_t* ptr = buffer;
				while (read_bytes) {
					ssize_t written_bytes = write(STDOUT_FILENO, ptr, read_bytes);
					if (written_bytes <= 0) {
						perror("write");
						return 1;
					}
					read_bytes -= written_bytes;
				}
			}
			return 0;
		}
	`

	// RunningStampFilename is the name of the file that is placed in a CI run
	// directory to indicate that it is still running.
	RunningStampFilename = ".running"
)

var (
	// ErrSkipped is an error category which causes the CI run to be marked as
	// skipped (since the tests/tests.json file or the settings.json files were
	// missing).
	ErrSkipped = stderrors.New("not found")
)

// State represents the state of the CI run or any of the individual tests.
type State int

var _ fmt.Stringer = State(0)
var _ json.Marshaler = State(0)

const (
	// StateWaiting marks the run/test to be waiting for the processing queue
	// to pick it up.
	StateWaiting State = iota
	// StateRunning signals that the request has been taken off the queue and
	// is currently running.
	StateRunning
	// StateSkipped signals that the CI run did not even start running since
	// the requested commit was not set up for CI, either because
	// tests/tests.json or settings.json were missing.
	StateSkipped
	// StateError signals that the CI run is no longer running because an error
	// ocurred.
	StateError
	// StateFailed signals that the CI run has finished running and it failed.
	StateFailed
	// StatePassed signals that the CI run has finished running and it was
	// successful.
	StatePassed
)

// String implements the fmt.Stringer interface.
func (s State) String() string {
	if s == StateWaiting {
		return "waiting"
	}
	if s == StateRunning {
		return "running"
	}
	if s == StateSkipped {
		return "skipped"
	}
	if s == StateError {
		return "error"
	}
	if s == StatePassed {
		return "passed"
	}
	return "failed"
}

// MarshalJSON implements the json.Marshaler interface.
func (s State) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", s)), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *State) UnmarshalJSON(data []byte) error {
	if string(data) == "\"waiting\"" {
		*s = StateWaiting
		return nil
	}
	if string(data) == "\"running\"" {
		*s = StateRunning
		return nil
	}
	if string(data) == "\"skipped\"" {
		*s = StateSkipped
		return nil
	}
	if string(data) == "\"error\"" {
		*s = StateError
		return nil
	}
	if string(data) == "\"passed\"" {
		*s = StatePassed
		return nil
	}
	*s = StateFailed
	return nil
}

// ReportError is a wrapper around error such that it can be marshaled to JSON.
type ReportError struct {
	Error error
}

var _ encoding.TextMarshaler = &ReportError{}
var _ encoding.TextUnmarshaler = &ReportError{}

// MarshalText implements the encoding.TextMarshaler interface.
func (s *ReportError) MarshalText() ([]byte, error) {
	return []byte(s.Error.Error()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (s *ReportError) UnmarshalText(text []byte) error {
	s.Error = errors.New(string(text))
	return nil
}

// ReportTest represents the result of an individual test case within the CI run.
type ReportTest struct {
	Index                  int                             `json:"index"`
	Type                   string                          `json:"type"`
	Filename               string                          `json:"filename"`
	EphemeralToken         string                          `json:"ephemeral_token,omitempty"`
	StartTime              time.Time                       `json:"start_time"`
	FinishTime             *time.Time                      `json:"finish_time,omitempty"`
	Duration               *base.Duration                  `json:"duration,omitempty"`
	State                  State                           `json:"state"`
	ReportError            *ReportError                    `json:"error,omitempty"`
	SolutionSetting        *common.SolutionSettings        `json:"solution,omitempty"`
	InputsValidatorSetting *common.InputsValidatorSettings `json:"inputs,omitempty"`
	Result                 *runner.RunResult               `json:"result,omitempty"`
}

// SetResult sets the result of running the test. It also updates the state of
// the test based on the verdict and score of the test.
func (t *ReportTest) SetResult(result *runner.RunResult) {
	t.Result = result

	if t.SolutionSetting != nil {
		if t.SolutionSetting.Verdict != "" && t.SolutionSetting.Verdict != result.Verdict {
			t.ReportError = &ReportError{
				Error: errors.Errorf(
					"expected verdict to be %q, got %q",
					t.SolutionSetting.Verdict,
					result.Verdict,
				),
			}
			t.State = StateFailed
			return
		}
		if t.SolutionSetting.ScoreRange != nil &&
			(t.SolutionSetting.ScoreRange.Min.Cmp(result.Score) > 0 ||
				t.SolutionSetting.ScoreRange.Max.Cmp(result.Score) < 0) {
			t.ReportError = &ReportError{
				Error: errors.Errorf(
					"expected score to be in range [%.3f, %.3f], got %.3f",
					base.RationalToFloat(t.SolutionSetting.ScoreRange.Min),
					base.RationalToFloat(t.SolutionSetting.ScoreRange.Max),
					base.RationalToFloat(result.Score),
				),
			}
			t.State = StateFailed
			return
		}
		if !t.SolutionSetting.AllowFractionalPercentages &&
			result.Score != nil &&
			(&big.Int{}).Rem(big.NewInt(100), result.Score.Denom()).Cmp(&big.Int{}) != 0 {
			t.ReportError = &ReportError{
				Error: errors.Errorf(
					"expected score to be an integer percentage, got %.6f%%",
					base.RationalToFloat(result.Score)*100,
				),
			}
			t.State = StateFailed
			return
		}
	} else {
		if result.Verdict != "AC" {
			t.ReportError = &ReportError{
				Error: errors.Errorf(
					"expected verdict to be \"AC\", got %q",
					result.Verdict,
				),
			}
			t.State = StateFailed
			return
		}
	}
	t.State = StatePassed
}

// String implements the fmt.Stringer interface.
func (t *ReportTest) String() string {
	if t == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", *t)
}

// Report represents the result of a CI run.
type Report struct {
	Problem     string         `json:"problem"`
	Hash        string         `json:"commit_hash"`
	IsEphemeral bool           `json:"is_ephemeral"`
	StartTime   time.Time      `json:"start_time"`
	FinishTime  *time.Time     `json:"finish_time,omitempty"`
	Duration    *base.Duration `json:"duration,omitempty"`
	State       State          `json:"state"`
	ReportError *ReportError   `json:"error,omitempty"`
	Tests       []*ReportTest  `json:"tests,omitempty"`
}

// UpdateState should be called when all of the tests have finished running.
func (r *Report) UpdateState() {
	for _, test := range r.Tests {
		if r.State > test.State {
			r.State = test.State
		}
	}
}

// Write serializes the gzipped report to the specified path. It does so by
// writing the report first to a temporary file and then atomically renames it
// to replace any pre-existing report.
func (r *Report) Write(reportPath string) error {
	reportDir := path.Dir(reportPath)
	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return errors.Wrap(
			err,
			"failed to create report directory",
		)
	}
	fd, err := ioutil.TempFile(reportDir, path.Base(reportPath))
	if err != nil {
		return errors.Wrap(
			err,
			"failed to open temporary file",
		)
	}
	defer fd.Close()
	tempPath := fd.Name()
	zw := gzip.NewWriter(fd)
	encoder := json.NewEncoder(zw)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(r); err != nil {
		zw.Close()
		os.Remove(tempPath)
		return errors.Wrap(
			err,
			"failed to serialize JSON report",
		)
	}
	if err = zw.Close(); err != nil {
		os.Remove(tempPath)
		return errors.Wrap(
			err,
			"failed to flush the gzip stream",
		)
	}

	return errors.Wrap(
		os.Rename(tempPath, reportPath),
		"failed to rename the temporary file",
	)
}

// SolutionConfig represents the configuration of a solution.
type SolutionConfig struct {
	Source   string
	Language string
}

// String implements the fmt.Stringer interface.
func (c *SolutionConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", *c)
}

// TestConfig represents the configuration of a single test within a tests.json file.
type TestConfig struct {
	Test     *ReportTest
	Solution SolutionConfig
	Input    *common.LiteralInput
}

// String implements the fmt.Stringer interface.
func (c *TestConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", *c)
}

// OutGeneratorConfig represents the configuration of the .out file generation.
type OutGeneratorConfig struct {
	Solution SolutionConfig
	Input    *common.LiteralInput
}

// String implements the fmt.Stringer interface.
func (c *OutGeneratorConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", *c)
}

// RunConfig represents the configuration of a suite of tests that need to run
// for a problem.
type RunConfig struct {
	TestsSettings      common.TestsSettings
	OutGeneratorConfig *OutGeneratorConfig
	TestConfigs        []*TestConfig

	// Input is the common.LiteralInput shared by all the non-input-validator
	// TestConfigs.
	Input *common.LiteralInput
}

// NewRunConfig creates a RunConfig based on the contents of ProblemFiles.
func NewRunConfig(files common.ProblemFiles, generateOutputFiles bool) (*RunConfig, error) {
	config := &RunConfig{
		Input: &common.LiteralInput{
			Cases: make(map[string]*common.LiteralCaseSettings),
		},
	}

	// Official solutions
	var solution *SolutionConfig
	{
		var solutions []string
		for _, filename := range files.Files() {
			filenameExtension := strings.SplitN(filename, ".", 2)
			if len(filenameExtension) == 2 && filenameExtension[0] == "solutions/solution" {
				solutions = append(solutions, filenameExtension[1])
			}
		}
		if len(solutions) > 1 {
			return nil, errors.Errorf(
				"multiple solutions/solution.* files for %s",
				files.String(),
			)
		}
		if len(solutions) == 1 {
			solution = &SolutionConfig{
				Language: solutions[0],
			}
			var err error
			if solution.Source, err = files.GetStringContents(
				fmt.Sprintf("solutions/solution.%s", solutions[0]),
			); err != nil {
				return nil, err
			}
		}
	}

	// Settings
	testsJSONContents, err := files.GetContents("tests/tests.json")
	if err != nil {
		return nil, base.ErrorWithCategory(
			ErrSkipped,
			err,
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(testsJSONContents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config.TestsSettings); err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to unmarshal tests/tests.json for %s",
			files.String(),
		)
	}

	problemSettings := common.ProblemSettings{
		Limits: common.DefaultLimits,
	}
	settingsJSONContents, err := files.GetContents("settings.json")
	if err != nil {
		return nil, base.ErrorWithCategory(
			ErrSkipped,
			err,
		)
	}
	if err := json.Unmarshal(settingsJSONContents, &problemSettings); err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to unmarshal settings.json for %s",
			files.String(),
		)
	}

	if len(problemSettings.Cases) == 0 {
		var err error
		problemSettings.Cases, err = common.GetGroupSettingsForProblem(files)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"failed to get group settings for %s",
				files.String(),
			)
		}
	}
	config.Input.Limits = &problemSettings.Limits

	invalidInputCases := make(map[string]*common.LiteralCaseSettings)

	// Read the cases into config.Input.Cases, as well as test-only invalid cases
	for _, kindSettings := range []struct {
		containingDirectory        string
		caseSettings               []common.GroupSettings
		caseDataDest               map[string]*common.LiteralCaseSettings
		generateOutputFiles        bool
		mustHaveAssociatedFailures bool
	}{
		{"cases", problemSettings.Cases, config.Input.Cases, generateOutputFiles, false},
		{"tests/invalid-cases", getInvalidCaseSettingsForValidation(files), invalidInputCases, false, true},
	} {
		for _, groupSettings := range kindSettings.caseSettings {
			for _, caseSettings := range groupSettings.Cases {
				literalCaseSettings := &common.LiteralCaseSettings{
					Weight: caseSettings.Weight,
				}

				if literalCaseSettings.Input, err = files.GetStringContents(
					fmt.Sprintf("%s/%s.in", kindSettings.containingDirectory, caseSettings.Name),
				); err != nil {
					return nil, errors.Wrapf(
						err,
						"failed to get case information for %s %s",
						files.String(),
						caseSettings.Name,
					)
				}

				outFilename := fmt.Sprintf("%s/%s.out", kindSettings.containingDirectory, caseSettings.Name)
				if kindSettings.generateOutputFiles {
					f, err := files.Open(outFilename)
					if f != nil {
						f.Close()
					}
					if !os.IsNotExist(err) {
						return nil, errors.Errorf(
							".out generation and existing .out files are not compatible: found %s in %s",
							outFilename,
							files.String(),
						)
					}
				} else {
					if literalCaseSettings.ExpectedOutput, err = files.GetStringContents(
						outFilename,
					); err != nil {
						return nil, errors.Wrapf(
							err,
							"failed to get case information for %s %s",
							files.String(),
							caseSettings.Name,
						)
					}
				}

				literalCaseSettings.ExpectedValidatorStderr, err = files.GetStringContents(
					fmt.Sprintf("%s/%s.expected-failure", kindSettings.containingDirectory, caseSettings.Name),
				)

				if err != nil && kindSettings.mustHaveAssociatedFailures {
					return nil, errors.Wrapf(
						err,
						"couldn't find associated failure message for %s %s",
						files.String(),
						caseSettings.Name,
					)
				} else if err == nil && !kindSettings.mustHaveAssociatedFailures {
					return nil, errors.Errorf(
						"found unexpected associated failure file for %s %s",
						files.String(),
						caseSettings.Name,
					)
				}

				kindSettings.caseDataDest[caseSettings.Name] = literalCaseSettings
			}
		}
	}

	if config.TestsSettings.ExpectedMaxScore != nil {
		expectedScore := (*big.Rat)(config.TestsSettings.ExpectedMaxScore)
		totalScore := big.NewRat(0, 1)
		for _, settings := range config.Input.Cases {
			totalScore.Add(totalScore, settings.Weight)
		}
		if totalScore.Cmp(expectedScore) != 0 {
			return nil, errors.Errorf(
				"max score doesn't match: expected %s, got %s",
				expectedScore.String(),
				totalScore.String(),
			)
		}
	}

	// Validator
	config.Input.Validator = &common.LiteralValidatorSettings{
		Name:             problemSettings.Validator.Name,
		Tolerance:        problemSettings.Validator.Tolerance,
		GroupScorePolicy: problemSettings.Validator.GroupScorePolicy,
	}
	if problemSettings.Validator.Name == common.ValidatorNameCustom {
		if problemSettings.Validator.Lang == nil {
			var validators []string
			for _, filename := range files.Files() {
				filenameExtension := strings.SplitN(filename, ".", 2)
				if len(filenameExtension) == 2 && filenameExtension[0] == "validator" {
					validators = append(validators, filenameExtension[1])
				}
			}

			if len(validators) == 0 {
				return nil, errors.Errorf(
					"failed to get validator language for %s",
					files.String(),
				)
			}
			if len(validators) > 1 {
				return nil, errors.Errorf(
					"multiple validator.* files for %s",
					files.String(),
				)
			}
			problemSettings.Validator.Lang = &validators[0]
		}
		customValidatorSettings := &common.LiteralCustomValidatorSettings{
			Language: *problemSettings.Validator.Lang,
			Limits:   problemSettings.Validator.Limits,
		}

		if customValidatorSettings.Source, err = files.GetStringContents(
			fmt.Sprintf("validator.%s", *problemSettings.Validator.Lang),
		); err != nil {
			return nil, err
		}

		config.Input.Validator.CustomValidator = customValidatorSettings
	}

	// Interactive
	if problemSettings.Interactive != nil {
		config.Input.Interactive = &common.LiteralInteractiveSettings{
			ModuleName: problemSettings.Interactive.ModuleName,
			ParentLang: problemSettings.Interactive.ParentLang,
			Templates:  problemSettings.Interactive.Templates,
		}
		if config.Input.Interactive.IDLSource, err = files.GetStringContents(
			fmt.Sprintf(
				"interactive/%s.idl",
				common.LanguageFileExtension(config.Input.Interactive.ModuleName),
			),
		); err != nil {
			return nil, err
		}
		if config.Input.Interactive.MainSource, err = files.GetStringContents(
			fmt.Sprintf(
				"interactive/Main.%s",
				common.LanguageFileExtension(config.Input.Interactive.ParentLang),
			),
		); err != nil {
			return nil, err
		}
	}

	// Report tests
	for _, solutionSetting := range config.TestsSettings.Solutions {
		language := solutionSetting.Language
		if language == "" {
			ext := filepath.Ext(solutionSetting.Filename)
			if ext == "" {
				return nil, errors.Errorf(
					"failed to get solution language for %s in %s",
					solutionSetting.Filename,
					files.String(),
				)
			}
			language = common.FileExtensionLanguage(ext[1:])
		}
		solutionSettingCopy := solutionSetting
		testConfig := &TestConfig{
			Test: &ReportTest{
				Index:           len(config.TestConfigs),
				Type:            "solutions",
				Filename:        solutionSetting.Filename,
				SolutionSetting: &solutionSettingCopy,
			},
			Input: config.Input,
			Solution: SolutionConfig{
				Language: language,
			},
		}
		if testConfig.Solution.Source, err = files.GetStringContents(
			fmt.Sprintf("tests/%s", solutionSetting.Filename),
		); err != nil {
			return nil, err
		}
		config.TestConfigs = append(config.TestConfigs, testConfig)
	}

	if config.TestsSettings.InputsValidator != nil {
		language := config.TestsSettings.InputsValidator.Language
		if language == "" {
			ext := filepath.Ext(config.TestsSettings.InputsValidator.Filename)
			if ext == "" {
				return nil, errors.Errorf(
					"failed to get input validator language for %s in %s",
					config.TestsSettings.InputsValidator.Filename,
					files.String(),
				)
			}
			language = common.FileExtensionLanguage(ext[1:])
		}

		for _, params := range []struct {
			reportType       string
			cases            map[string]*common.LiteralCaseSettings
			expectedSolution *common.SolutionSettings
		}{
			// Real test cases to validate
			{"inputs", config.Input.Cases, nil},
			// Known invalid test cases
			{"invalid-inputs", invalidInputCases, &common.SolutionSettings{Verdict: "WA"}},
		} {
			params := params

			if params.reportType == "invalid-inputs" && len(params.cases) == 0 {
				continue
			}

			testConfig := &TestConfig{
				Test: &ReportTest{
					Index:                  len(config.TestConfigs),
					Type:                   params.reportType,
					Filename:               config.TestsSettings.InputsValidator.Filename,
					InputsValidatorSetting: config.TestsSettings.InputsValidator,
					SolutionSetting:        params.expectedSolution,
				},
				Solution: SolutionConfig{
					Source:   CopyStdinToStdoutSource,
					Language: "cpp11",
				},
				Input: &common.LiteralInput{
					Cases: params.cases,
					Validator: &common.LiteralValidatorSettings{
						Name: common.ValidatorNameCustom,
						CustomValidator: &common.LiteralCustomValidatorSettings{
							Language: language,
						},
					},
				},
			}
			if testConfig.Input.Validator.CustomValidator.Source, err = files.GetStringContents(
				fmt.Sprintf("tests/%s", config.TestsSettings.InputsValidator.Filename),
			); err != nil {
				return nil, err
			}
			config.TestConfigs = append(config.TestConfigs, testConfig)
		}
	}

	// .out generation
	if generateOutputFiles {
		if solution == nil {
			return nil, errors.Errorf(
				"missing solutions/solution.* files for .out generation in %s",
				files.String(),
			)
		}
		config.OutGeneratorConfig = &OutGeneratorConfig{
			Solution: *solution,
			Input: &common.LiteralInput{
				Cases:     make(map[string]*common.LiteralCaseSettings),
				Limits:    config.Input.Limits,
				Validator: config.Input.Validator,
			},
		}
		for _, filename := range files.Files() {
			if !strings.HasSuffix(filename, ".in") {
				continue
			}
			if !strings.HasPrefix(filename, "cases/") &&
				!strings.HasPrefix(filename, "examples/") &&
				!strings.HasPrefix(filename, "statements/") {
				continue
			}

			inputContents, err := files.GetStringContents(filename)
			if err != nil {
				return nil, errors.Wrapf(
					err,
					"failed to get input contents for %s %s",
					files.String(),
					filename,
				)
			}

			config.OutGeneratorConfig.Input.Cases[strings.TrimSuffix(filename, ".in")] = &common.LiteralCaseSettings{
				Weight: big.NewRat(1, 1),
				Input:  inputContents,
			}
		}
	}

	return config, nil
}

type sizedEntry struct {
	path string
	size base.Byte
	log  logging.Logger
}

var _ base.SizedEntry = &sizedEntry{}

func (e *sizedEntry) Release() {
	if err := os.RemoveAll(e.path); err != nil {
		e.log.Error(
			"Evicting CI run failed",
			map[string]interface{}{
				"path": e.path,
				"err":  err,
			},
		)
	}
}

func (e *sizedEntry) Size() base.Byte {
	return e.size
}

func getDirectorySize(root string) (base.Byte, error) {
	size := int64(0)
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	}); err != nil {
		return base.Byte(0), err
	}
	return base.Byte(size), nil
}

// LRUCache is a base.LRUCache specialized for CI runs.
type LRUCache struct {
	*base.LRUCache
	log logging.Logger
}

// NewLRUCache returns a new LRUCache with the specified size limit.
func NewLRUCache(sizeLimit base.Byte, log logging.Logger) *LRUCache {
	return &LRUCache{
		LRUCache: base.NewLRUCache(sizeLimit),
		log:      log,
	}
}

// AddRun adds a run (which is a directory name with files) into the LRUCache.
func (l *LRUCache) AddRun(currentPath string, key string) {
	ref, err := l.Get(
		key,
		func(hash string) (base.SizedEntry, error) {
			size, err := getDirectorySize(currentPath)
			if err != nil {
				return nil, err
			}
			return &sizedEntry{
				path: currentPath,
				size: size,
				log:  l.log,
			}, nil
		},
	)
	if err != nil {
		l.log.Error(
			"Error adding path to LRU cache. Removing instead",
			map[string]interface{}{
				"path": currentPath,
				"err":  err,
			},
		)
		if err := os.RemoveAll(currentPath); err != nil {
			l.log.Error(
				"Removing errored run failed",
				map[string]interface{}{
					"path": currentPath,
					"err":  err,
				},
			)
		}
		return
	}
	// Release immediately so that the entry can be evicted.
	l.Put(ref)
}

// ReloadRuns adds all CI runs that are in the ciRoot directory to the LRUCache.
func (l *LRUCache) ReloadRuns(ciRoot string) error {
	return filepath.Walk(ciRoot, func(currentPath string, info os.FileInfo, err error) error {
		rel, err := filepath.Rel(ciRoot, currentPath)
		if err != nil {
			return err
		}
		components := strings.Split(rel, string(filepath.Separator))
		if len(components) < 3 || !info.IsDir() {
			return nil
		}

		if _, err := os.Stat(path.Join(currentPath, RunningStampFilename)); err == nil {
			// The existence of the .running stamp file means that a previous run of
			// the grader did not finish a CI run, so we will not be able to
			// continue it. Remove the whole directory.
			if err := os.RemoveAll(currentPath); err != nil {
				l.log.Error(
					"Removing unfinished run failed",
					map[string]interface{}{
						"path": currentPath,
						"err":  err,
					},
				)
			}
			return filepath.SkipDir
		}

		l.AddRun(
			currentPath,
			fmt.Sprintf("%s/%s%s", components[0], components[1], components[2]),
		)
		return filepath.SkipDir
	})
}

func getInvalidCaseSettingsForValidation(f common.ProblemFiles) []common.GroupSettings {
	invalidCasesRegexp := regexp.MustCompile("^tests/invalid-cases/([^/]+)\\.in$")
	caseWeightMapping := common.NewCaseWeightMapping()

	for _, filename := range f.Files() {
		casesMatches := invalidCasesRegexp.FindStringSubmatch(filename)
		if casesMatches == nil {
			continue
		}
		caseName := casesMatches[1]

		caseWeightMapping.AddCaseName(caseName, big.NewRat(1, 1), false)
	}

	return caseWeightMapping.ToGroupSettings()
}
