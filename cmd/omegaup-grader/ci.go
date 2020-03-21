package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/inconshreveable/log15"
	git "github.com/lhchavez/git2go/v29"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
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

	runningStampFilename = ".running"
)

var (
	ciURLRegexp = regexp.MustCompile(`^/ci/problem/([a-zA-Z0-9-_]+)/([0-9a-f]{40})/$`)

	// ErrSkipped is an error category which causes the CI run to be marked as
	// skipped (since the tests/tests.json file or the settings.json files were
	// missing).
	ErrSkipped = stderrors.New("not found")
)

// CIState represents the state of the CI run or any of the individual tests.
type CIState int

var _ fmt.Stringer = CIState(0)
var _ json.Marshaler = CIState(0)

const (
	// CIStateWaiting marks the run/test to be waiting for the processing queue
	// to pick it up.
	CIStateWaiting CIState = iota
	// CIStateRunning signals that the request has been taken off the queue and
	// is currently running.
	CIStateRunning
	// CIStateSkipped signals that the CI run did not even start running since
	// the requested commit was not set up for CI, either because
	// tests/tests.json or settings.json were missing.
	CIStateSkipped
	// CIStateError signals that the CI run is no longer running because an error
	// ocurred.
	CIStateError
	// CIStatePassed signals that the CI run has finished running and it was
	// successful.
	CIStatePassed
	// CIStateFailed signals that the CI run has finished running and it failed.
	CIStateFailed
)

// String implements the fmt.Stringer interface.
func (s CIState) String() string {
	if s == CIStateWaiting {
		return "waiting"
	}
	if s == CIStateRunning {
		return "running"
	}
	if s == CIStateSkipped {
		return "skipped"
	}
	if s == CIStateError {
		return "error"
	}
	if s == CIStatePassed {
		return "passed"
	}
	return "failed"
}

// MarshalJSON implements the json.Marshaler interface.
func (s CIState) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", s)), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *CIState) UnmarshalJSON(data []byte) error {
	if string(data) == "\"waiting\"" {
		*s = CIStateWaiting
		return nil
	}
	if string(data) == "\"running\"" {
		*s = CIStateRunning
		return nil
	}
	if string(data) == "\"skipped\"" {
		*s = CIStateSkipped
		return nil
	}
	if string(data) == "\"error\"" {
		*s = CIStateError
		return nil
	}
	if string(data) == "\"passed\"" {
		*s = CIStatePassed
		return nil
	}
	*s = CIStateFailed
	return nil
}

type ciRunConfig struct {
	input         *common.LiteralInput
	testsSettings common.TestsSettings
	reportTests   []*CIReportTest
}

// CIReportTest represents the result of an individual test case within the CI run.
type CIReportTest struct {
	Type                   string                          `json:"type"`
	Filename               string                          `json:"filename"`
	EphemeralToken         string                          `json:"ephemeral_token,omitempty"`
	StartTime              time.Time                       `json:"start_time"`
	FinishTime             *time.Time                      `json:"finish_time,omitempty"`
	Duration               *base.Duration                  `json:"duration,omitempty"`
	State                  CIState                         `json:"state"`
	Error                  string                          `json:"error,omitempty"`
	SolutionSetting        *common.SolutionSettings        `json:"solution,omitempty"`
	InputsValidatorSetting *common.InputsValidatorSettings `json:"inputs,omitempty"`
	Result                 *runner.RunResult               `json:"result,omitempty"`

	ephemeralRunRequest *grader.EphemeralRunRequest
}

// CIReport represents the result of a CI run.
type CIReport struct {
	Problem    string          `json:"problem"`
	CommitHash string          `json:"commit_hash"`
	StartTime  time.Time       `json:"start_time"`
	FinishTime *time.Time      `json:"finish_time,omitempty"`
	Duration   *base.Duration  `json:"duration,omitempty"`
	State      CIState         `json:"state"`
	Error      string          `json:"error,omitempty"`
	Tests      []*CIReportTest `json:"tests,omitempty"`

	reportDir  string
	reportPath string
}

func (r *CIReport) Write() error {
	fd, err := ioutil.TempFile(r.reportDir, "")
	if err != nil {
		return errors.Wrap(
			err,
			"failed to open temporary file",
		)
	}
	tempPath := fd.Name()
	zw := gzip.NewWriter(fd)
	encoder := json.NewEncoder(zw)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(r); err != nil {
		zw.Close()
		fd.Close()
		os.Remove(tempPath)
		return errors.Wrap(
			err,
			"failed to serialize JSON report",
		)
	}
	if err = zw.Close(); err != nil {
		fd.Close()
		os.Remove(tempPath)
		return errors.Wrap(
			err,
			"failed to flush the gzip stream",
		)
	}
	fd.Close()

	return errors.Wrap(
		os.Rename(tempPath, r.reportPath),
		"failed to rename the temporary file",
	)
}

func unmarshalJSON(
	repository *git.Repository,
	commitHash string,
	tree *git.Tree,
	path string,
	v interface{},
) error {
	entry, err := tree.EntryByPath(path)
	if err != nil {
		return errors.Wrapf(
			err,
			"failed to find %s for %s:%s",
			path,
			repository.Path(),
			commitHash,
		)
	}
	obj, err := repository.LookupBlob(entry.Id)
	if err != nil {
		return errors.Wrapf(
			err,
			"failed to lookup %s for %s:%s",
			path,
			repository.Path(),
			commitHash,
		)
	}
	defer obj.Free()
	if err := json.Unmarshal(obj.Contents(), v); err != nil {
		return errors.Wrapf(
			err,
			"failed to unmarshal %s for %s:%s",
			path,
			repository.Path(),
			commitHash,
		)
	}

	return nil
}

func getBlobContents(
	repository *git.Repository,
	commitHash string,
	tree *git.Tree,
	path string,
) (string, error) {
	entry, err := tree.EntryByPath(path)
	if err != nil {
		return "", errors.Wrapf(
			err,
			"failed to find %s for %s:%s",
			path,
			repository.Path(),
			commitHash,
		)
	}
	obj, err := repository.LookupBlob(entry.Id)
	if err != nil {
		return "", errors.Wrapf(
			err,
			"failed to lookup %s for %s:%s",
			path,
			repository.Path(),
			commitHash,
		)
	}
	defer obj.Free()

	return string(obj.Contents()), nil
}

func createCIRunConfigFromGit(
	ctx *grader.Context,
	repositoryPath string,
	commitHash string,
) (*ciRunConfig, error) {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to open repository %s:%s",
			repository.Path(),
			commitHash,
		)
	}
	defer repository.Free()
	commitID, err := git.NewOid(commitHash)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to parse commit id %s:%s",
			repository.Path(),
			commitHash,
		)
	}
	commit, err := repository.LookupCommit(commitID)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to lookup commit %s:%s",
			repository.Path(),
			commitHash,
		)
	}
	defer commit.Free()
	tree, err := commit.Tree()
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to get tree for commit %s:%s",
			repository.Path(),
			commit.Id(),
		)
	}

	config := &ciRunConfig{
		input: &common.LiteralInput{
			Cases: make(map[string]*common.LiteralCaseSettings),
		},
	}

	if err := unmarshalJSON(
		repository,
		commit.Id().String(),
		tree,
		"tests/tests.json",
		&config.testsSettings,
	); err != nil {
		return nil, base.ErrorWithCategory(
			ErrSkipped,
			err,
		)
	}

	var problemSettings common.ProblemSettings
	if err := unmarshalJSON(
		repository,
		commit.Id().String(),
		tree,
		"settings.json",
		&problemSettings,
	); err != nil {
		return nil, base.ErrorWithCategory(
			ErrSkipped,
			err,
		)
	}

	config.input.Limits = &problemSettings.Limits

	// Cases
	for _, groupSettings := range problemSettings.Cases {
		for _, caseSettings := range groupSettings.Cases {
			literalCaseSettings := &common.LiteralCaseSettings{
				Weight: caseSettings.Weight,
			}

			if literalCaseSettings.Input, err = getBlobContents(
				repository,
				commit.Id().String(),
				tree,
				fmt.Sprintf("cases/%s.in", caseSettings.Name),
			); err != nil {
				return nil, errors.Wrapf(
					err,
					"failed to get case information for %s:%s %s",
					repository.Path(),
					commit.Id(),
					caseSettings.Name,
				)
			}

			if literalCaseSettings.ExpectedOutput, err = getBlobContents(
				repository,
				commit.Id().String(),
				tree,
				fmt.Sprintf("cases/%s.out", caseSettings.Name),
			); err != nil {
				return nil, errors.Wrapf(
					err,
					"failed to get case information for %s:%s %s",
					repository.Path(),
					commit.Id(),
					caseSettings.Name,
				)
			}

			config.input.Cases[caseSettings.Name] = literalCaseSettings
		}
	}

	// Validator
	config.input.Validator = &common.LiteralValidatorSettings{
		Name:      problemSettings.Validator.Name,
		Tolerance: problemSettings.Validator.Tolerance,
	}
	if problemSettings.Validator.Name == "custom" {
		if problemSettings.Validator.Lang == nil {
			return nil, errors.Wrapf(
				err,
				"failed to get validator language for %s:%s",
				repository.Path(),
				commit.Id(),
			)
		}
		customValidatorSettings := &common.LiteralCustomValidatorSettings{
			Language: *problemSettings.Validator.Lang,
			Limits:   problemSettings.Validator.Limits,
		}

		if customValidatorSettings.Source, err = getBlobContents(
			repository,
			commit.Id().String(),
			tree,
			fmt.Sprintf("validator.%s", *problemSettings.Validator.Lang),
		); err != nil {
			return nil, err
		}

		config.input.Validator.CustomValidator = customValidatorSettings
	}

	// Interactive
	if problemSettings.Interactive != nil {
		config.input.Interactive = &common.LiteralInteractiveSettings{
			ModuleName: problemSettings.Interactive.ModuleName,
			ParentLang: problemSettings.Interactive.ParentLang,
			Templates:  problemSettings.Interactive.Templates,
		}
		if config.input.Interactive.IDLSource, err = getBlobContents(
			repository,
			commit.Id().String(),
			tree,
			fmt.Sprintf(
				"interactive/%s.idl",
				common.LanguageFileExtension(config.input.Interactive.ModuleName),
			),
		); err != nil {
			return nil, err
		}
		if config.input.Interactive.MainSource, err = getBlobContents(
			repository,
			commit.Id().String(),
			tree,
			fmt.Sprintf(
				"interactive/Main.%s",
				common.LanguageFileExtension(config.input.Interactive.ParentLang),
			),
		); err != nil {
			return nil, err
		}
	}

	// Report tests
	for _, solutionSetting := range config.testsSettings.Solutions {
		language := solutionSetting.Language
		if language == "" {
			ext := filepath.Ext(solutionSetting.Filename)
			if ext == "" {
				return nil, errors.Errorf(
					"failed to get solution language for %s in %s:%s",
					solutionSetting.Filename,
					repository.Path(),
					commit.Id(),
				)
			}
			language = common.FileExtensionLanguage(ext[1:])
		}
		solutionSettingCopy := solutionSetting
		reportTest := &CIReportTest{
			Type:     "solutions",
			Filename: solutionSetting.Filename,
			ephemeralRunRequest: &grader.EphemeralRunRequest{
				Language: language,
				Input:    config.input,
			},
			SolutionSetting: &solutionSettingCopy,
		}
		if reportTest.ephemeralRunRequest.Source, err = getBlobContents(
			repository,
			commit.Id().String(),
			tree,
			fmt.Sprintf("tests/%s", solutionSetting.Filename),
		); err != nil {
			return nil, err
		}
		config.reportTests = append(config.reportTests, reportTest)
	}

	if config.testsSettings.InputsValidator != nil {
		language := config.testsSettings.InputsValidator.Language
		if language == "" {
			ext := filepath.Ext(config.testsSettings.InputsValidator.Filename)
			if ext == "" {
				return nil, errors.Errorf(
					"failed to get input validator language for %s in %s:%s",
					config.testsSettings.InputsValidator.Filename,
					repository.Path(),
					commit.Id(),
				)
			}
			language = common.FileExtensionLanguage(ext[1:])
		}
		reportTest := &CIReportTest{
			Type:     "inputs",
			Filename: config.testsSettings.InputsValidator.Filename,
			ephemeralRunRequest: &grader.EphemeralRunRequest{
				Source:   CopyStdinToStdoutSource,
				Language: "cpp11",
				Input: &common.LiteralInput{
					Cases: config.input.Cases,
					Validator: &common.LiteralValidatorSettings{
						Name: "custom",
						CustomValidator: &common.LiteralCustomValidatorSettings{
							Language: language,
						},
					},
				},
			},
			InputsValidatorSetting: config.testsSettings.InputsValidator,
		}
		if reportTest.ephemeralRunRequest.Input.Validator.CustomValidator.Source, err = getBlobContents(
			repository,
			commit.Id().String(),
			tree,
			fmt.Sprintf("tests/%s", config.testsSettings.InputsValidator.Filename),
		); err != nil {
			return nil, err
		}
		config.reportTests = append(config.reportTests, reportTest)
	}

	return config, nil
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

type ciSizedEntry struct {
	path string
	size base.Byte
	log  log15.Logger
}

var _ base.SizedEntry = &ciSizedEntry{}

func (e *ciSizedEntry) Release() {
	if err := os.RemoveAll(e.path); err != nil {
		e.log.Error("Evicting CI run failed", "path", e.path, "err", err)
	}
}

func (e *ciSizedEntry) Size() base.Byte {
	return e.size
}

type ciHandler struct {
	ephemeralRunManager *grader.EphemeralRunManager
	ctx                 *grader.Context
	lruCache            *base.LRUCache
	stopChan            chan struct{}
	reportChan          chan *CIReport
	doneChan            chan struct{}
}

func (h *ciHandler) addRun(
	test *CIReportTest,
	runs *grader.Queue,
	report *CIReport,
) error {
	test.StartTime = time.Now()

	h.ctx.Metrics.CounterAdd("grader_ephemeral_runs_total", 1)
	h.ctx.Log.Debug("Adding new run", "run", test.ephemeralRunRequest)
	maxScore := &big.Rat{}
	for _, literalCase := range test.ephemeralRunRequest.Input.Cases {
		maxScore.Add(maxScore, literalCase.Weight)
	}
	inputFactory, err := common.NewLiteralInputFactory(
		test.ephemeralRunRequest.Input,
		h.ctx.Config.Grader.RuntimePath,
		common.LiteralPersistGrader,
	)
	if err != nil {
		h.ctx.Log.Error("Error creating input factory", "err", err)
		return err
	}
	input, err := h.ctx.InputManager.Add(inputFactory.Hash(), inputFactory)
	if err != nil {
		h.ctx.Log.Error("Error adding input", "err", err)
		return err
	}

	runCtx := grader.NewEmptyRunContext(h.ctx)
	runCtx.Run.InputHash = inputFactory.Hash()
	runCtx.Run.Language = test.ephemeralRunRequest.Language
	runCtx.Run.MaxScore = maxScore
	runCtx.Run.Source = test.ephemeralRunRequest.Source
	runCtx.Priority = grader.QueuePriorityEphemeral
	test.EphemeralToken, err = h.ephemeralRunManager.SetEphemeral(runCtx)
	if err != nil {
		h.ctx.Log.Error("Error making run ephemeral", "err", err)
		return err
	}

	committed := false
	defer func(committed *bool) {
		if *committed {
			return
		}
		if err := os.RemoveAll(runCtx.GradeDir); err != nil {
			h.ctx.Log.Error("Error cleaning up after run", "err", err)
		}
	}(&committed)

	if err = grader.AddRunContext(h.ctx, runCtx, input); err != nil {
		h.ctx.Log.Error("Failed to add run context", "err", err)
		return err
	}

	runs.AddRun(runCtx)
	h.ctx.Log.Info("enqueued run", "run", runCtx.Run)

	// Wait until a runner has picked the run up, or the run has been finished.
	select {
	case <-runCtx.Running():
		test.State = CIStateRunning
		if err := report.Write(); err != nil {
			h.ctx.Log.Error("Failed to write the report file", "filename", report.reportPath, "err", err)
		}
		break
	case <-runCtx.Ready():
	}
	<-runCtx.Ready()

	{
		finishTime := time.Now()
		test.FinishTime = &finishTime
		duration := base.Duration(test.FinishTime.Sub(test.StartTime))
		test.Duration = &duration
	}
	test.Result = &runCtx.RunInfo.Result

	test.State = CIStatePassed
	if test.SolutionSetting != nil {
		if test.SolutionSetting.Verdict != "" && test.SolutionSetting.Verdict != test.Result.Verdict {
			test.State = CIStateFailed
		}
		if test.SolutionSetting.ScoreRange != nil &&
			(test.SolutionSetting.ScoreRange.Min.Cmp(test.Result.Score) > 0 ||
				test.SolutionSetting.ScoreRange.Max.Cmp(test.Result.Score) < 0) {
			test.State = CIStateFailed
		}
	} else {
		if test.Result.Verdict != "AC" {
			test.State = CIStateFailed
		}
	}

	if err := report.Write(); err != nil {
		h.ctx.Log.Error("Failed to write the report file", "filename", report.reportPath, "err", err)
	}

	// Finally commit the run to the manager.
	if err = saveEphemeralRunRequest(h.ctx, runCtx, test.ephemeralRunRequest); err != nil {
		h.ctx.Log.Error("Failed to commit the original request", "err", err)
		return err
	}
	h.ephemeralRunManager.Commit(runCtx)
	committed = true
	h.ctx.Log.Info("Finished running ephemeral run", "token", test.EphemeralToken)

	return nil
}

func (h *ciHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ctx.Log.Info("CI request", "path", r.URL.Path)

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	match := ciURLRegexp.FindStringSubmatch(r.URL.Path)
	if match == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	report := &CIReport{
		Problem:    match[1],
		CommitHash: match[2],
		StartTime:  time.Now(),
		State:      CIStateWaiting,
	}

	report.reportPath = path.Join(
		h.ctx.Config.Grader.RuntimePath,
		"ci",
		report.Problem,
		report.CommitHash[:2],
		report.CommitHash[2:],
		"report.json.gz",
	)
	report.reportDir = path.Dir(report.reportPath)
	if fd, err := os.Open(report.reportPath); err == nil {
		defer fd.Close()

		st, err := fd.Stat()
		if err != nil {
			h.ctx.Log.Error("Failed to stat the file", "filename", report.reportPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Add("Content-Type", "application/json")
		w.Header().Add("Content-Encoding", "gzip")
		http.ServeContent(w, r, report.reportPath, st.ModTime(), fd)
		return
	}

	// Do the barest minimum checks before fully committing to making this CI
	// run.
	repository, err := git.OpenRepository(grader.GetRepositoryPath(
		h.ctx.Config.Grader.RuntimePath,
		report.Problem,
	))
	if err != nil {
		h.ctx.Log.Error(
			"failed to open repository",
			"filename", report.reportPath,
			"err", err,
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer repository.Free()
	commitID, err := git.NewOid(report.CommitHash)
	if err != nil {
		h.ctx.Log.Error(
			"failed to parse commit",
			"filename", report.reportPath,
			"commit", report.CommitHash,
			"err", err,
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	commit, err := repository.LookupCommit(commitID)
	if err != nil {
		h.ctx.Log.Error(
			"failed to lookup commit",
			"filename", report.reportPath,
			"commit", report.CommitHash,
			"err", err,
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer commit.Free()

	h.ctx.Metrics.CounterAdd("grader_ci_jobs_total", 1)

	if err := os.MkdirAll(report.reportDir, 0755); err != nil {
		h.ctx.Log.Error("Failed to create the report directory", "filename", report.reportPath, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	stamp, err := os.OpenFile(
		path.Join(report.reportDir, runningStampFilename),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0644,
	)
	if err != nil {
		h.ctx.Log.Error("Failed to create the running stamp", "filename", report.reportPath, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	stamp.Close()

	if err := report.Write(); err != nil {
		h.ctx.Log.Error("Failed to create the report file", "filename", report.reportPath, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(report); err != nil {
		h.ctx.Log.Error("Failed to write report", "err", err)
	}

	h.reportChan <- report
}

func (h *ciHandler) processCIRequest(report *CIReport, runs *grader.Queue) {
	h.ctx.Log.Info("running request", "report", report)
	ciRunConfig, err := createCIRunConfigFromGit(
		h.ctx,
		grader.GetRepositoryPath(
			h.ctx.Config.Grader.RuntimePath,
			report.Problem,
		),
		report.CommitHash,
	)
	if err != nil {
		h.ctx.Log.Error("Failed to validate commit", "err", err)
		if base.HasErrorCategory(err, ErrSkipped) {
			report.State = CIStateSkipped
		} else {
			report.State = CIStateError
		}
		report.Error = err.Error()
		{
			finishTime := time.Now()
			report.FinishTime = &finishTime
			duration := base.Duration(report.FinishTime.Sub(report.StartTime))
			report.Duration = &duration
		}
		if err := report.Write(); err != nil {
			h.ctx.Log.Error("Failed to write the report file", "filename", report.reportPath, "err", err)
		}
		return
	}
	report.Tests = ciRunConfig.reportTests
	if err := report.Write(); err != nil {
		h.ctx.Log.Error("Failed to write the report file", "filename", report.reportPath, "err", err)
	}

	report.State = CIStateRunning
	finalState := CIStatePassed
	for _, test := range report.Tests {
		if err := h.addRun(test, runs, report); err != nil {
			h.ctx.Log.Error("Failed to perform ephemeral run", "err", err)
			test.State = CIStateError
			test.Error = err.Error()
			finalState = test.State
		} else if finalState == CIStatePassed {
			finalState = test.State
		}
	}
	report.State = finalState

	{
		finishTime := time.Now()
		report.FinishTime = &finishTime
		duration := base.Duration(report.FinishTime.Sub(report.StartTime))
		report.Duration = &duration
	}
	h.ctx.Log.Info("running request", "report", report)
	if err := report.Write(); err != nil {
		h.ctx.Log.Error("Failed to write the report file", "filename", report.reportPath, "err", err)
	}

	h.ctx.Log.Info("finished running request", "report", report)

	if err := os.Remove(path.Join(report.reportDir, runningStampFilename)); err != nil {
		h.ctx.Log.Error("Failed to remove the running stamp", "filename", report.reportPath, "err", err)
	}

	h.addRunToCache(
		report.reportDir,
		fmt.Sprintf("%s/%s", report.Problem, report.CommitHash),
	)
}

func (h *ciHandler) addRunToCache(currentPath string, key string) {
	ref, err := h.lruCache.Get(
		key,
		func(hash string) (base.SizedEntry, error) {
			size, err := getDirectorySize(currentPath)
			if err != nil {
				return nil, err
			}
			return &ciSizedEntry{
				path: currentPath,
				size: size,
			}, nil
		},
	)
	if err != nil {
		h.ctx.Log.Error(
			"Error adding path to LRU cache. Removing instead",
			"path", currentPath,
			"err", err,
		)
		if err := os.RemoveAll(currentPath); err != nil {
			h.ctx.Log.Error(
				"Removing errored run failed",
				"path", currentPath,
				"err", err,
			)
		}
		return
	}
	// Release immediately so that the entry can be evicted.
	h.lruCache.Put(ref)
}

func (h *ciHandler) run() {
	runs, err := h.ctx.QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}

	ciRoot := path.Join(h.ctx.Config.Grader.RuntimePath, "ci")
	h.ctx.Log.Info("Reloading CI runs...")
	if err := filepath.Walk(ciRoot, func(currentPath string, info os.FileInfo, err error) error {
		rel, err := filepath.Rel(ciRoot, currentPath)
		if err != nil {
			return err
		}
		components := strings.Split(rel, string(filepath.Separator))
		if len(components) < 3 || !info.IsDir() {
			return nil
		}

		if _, err := os.Stat(path.Join(currentPath, runningStampFilename)); err == nil {
			// The existence of the .running stamp file means that a previous run of
			// the grader did not finish a CI run, so we will not be able to
			// continue it. Remove the whole directory.
			if err := os.RemoveAll(currentPath); err != nil {
				h.ctx.Log.Error("Removing unfinished run failed", "path", currentPath, "err", err)
			}
			return filepath.SkipDir
		}

		h.addRunToCache(
			currentPath,
			fmt.Sprintf("%s/%s%s", components[0], components[1], components[2]),
		)
		return filepath.SkipDir
	}); err != nil {
		h.ctx.Log.Error("Reloading CI runs failed", "err", err)
	}
	h.ctx.Log.Info("Finished preloading CI runs", "cache_size", h.lruCache.Size())

	h.ctx.Log.Info("CI run manager ready")
	for {
		select {
		case <-h.stopChan:
			close(h.doneChan)
			return

		case report := <-h.reportChan:
			h.processCIRequest(report, runs)
		}
	}
}

func (h *ciHandler) Shutdown(ctx context.Context) error {
	close(h.stopChan)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.doneChan:
	}
	return nil
}

func registerCIHandlers(
	ctx *grader.Context,
	mux *http.ServeMux,
	ephemeralRunManager *grader.EphemeralRunManager,
) shutdowner {
	ciHandler := &ciHandler{
		ephemeralRunManager: ephemeralRunManager,
		ctx:                 ctx,
		lruCache:            base.NewLRUCache(ctx.Config.Grader.CI.CISizeLimit),
		stopChan:            make(chan struct{}),
		reportChan:          make(chan *CIReport, 128),
		doneChan:            make(chan struct{}),
	}
	mux.Handle("/ci/", ciHandler)
	go ciHandler.run()
	return ciHandler
}
