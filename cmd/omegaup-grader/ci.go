package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/google/uuid"
	git "github.com/libgit2/git2go/v33"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner/ci"
)

var (
	ciGitURLRegexp       = regexp.MustCompile(`^/ci/problem/([a-zA-Z0-9-_]+)/([0-9a-f]{40})/$`)
	ciEphemeralURLRegexp = regexp.MustCompile(`^/ci/ephemeral/(([0-9a-f-]{36})/)?$`)
)

type startedCIRequest struct {
	report       *ci.Report
	path         string
	problemFiles *common.ProblemFiles
}

type ciHandler struct {
	ephemeralRunManager *grader.EphemeralRunManager
	ctx                 *grader.Context
	lruCache            *ci.LRUCache
	stopChan            chan struct{}
	requestChan         chan *startedCIRequest
	doneChan            chan struct{}
}

func (h *ciHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := h.ctx.Wrap(r.Context())

	ctx.Log.Info(
		"CI request",
		map[string]interface{}{
			"path": r.URL.Path,
		},
	)

	report := &ci.Report{
		StartTime: time.Now(),
		State:     ci.StateWaiting,
	}

	var expectedMethod string

	if match := ciGitURLRegexp.FindStringSubmatch(r.URL.Path); match != nil {

		report.IsEphemeral = false
		report.Problem = match[1]
		report.Hash = match[2]
		expectedMethod = http.MethodGet

	} else if match := ciEphemeralURLRegexp.FindStringSubmatch(r.URL.Path); match != nil {

		report.IsEphemeral = true
		report.Problem = "+ephemeral+"

		if len(match) > 3 {
			expectedMethod = http.MethodGet
			report.Hash = match[3]
		} else {
			expectedMethod = http.MethodPost
			report.Hash = uuid.New().String()
		}

	} else {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if r.Method != expectedMethod {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	reportPath := path.Join(
		ctx.Config.Grader.RuntimePath,
		"ci",
		report.Problem,
		report.Hash[:2],
		report.Hash[2:],
		"report.json.gz",
	)

	if fd, err := os.Open(reportPath); err == nil {
		defer fd.Close()

		st, err := fd.Stat()
		if err != nil {
			ctx.Log.Error(
				"Failed to stat the file",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.Header().Add("Content-Type", "application/json")
		w.Header().Add("Content-Encoding", "gzip")
		http.ServeContent(w, r, reportPath, st.ModTime(), fd)
		return
	}

	// Do the barest minimum checks before fully committing to making this CI
	// run.

	// todo(frcepeda): check with frontend for rate limiting

	var problemFiles *common.ProblemFiles

	if report.IsEphemeral {
		r.ParseMultipartForm((base.Byte(150) * base.Mebibyte).Bytes())
		problemZip, _, err := r.FormFile("problem.zip")

		if err != nil {
			ctx.Log.Error(
				"failed to load problem package",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer problemZip.Close()

		limit := (base.Byte(100) * base.Mebibyte).Bytes()
		buf := make([]byte, limit)
		bufSize, err := io.ReadFull(io.LimitReader(problemZip, limit), buf)

		if err != io.ErrUnexpectedEOF {
			ctx.Log.Error(
				"failed to load problem package",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		zipReader, err := zip.NewReader(bytes.NewReader(buf), int64(bufSize))
		if err != nil {
			ctx.Log.Error(
				"failed to load problem package",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		p := common.NewProblemFilesFromZip(zipReader, "<memory>")
		problemFiles = &p
	} else {
		repository, err := git.OpenRepository(grader.GetRepositoryPath(
			ctx.Config.Grader.RuntimePath,
			report.Problem,
		))
		if err != nil {
			ctx.Log.Error(
				"failed to open repository",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer repository.Free()
		commitID, err := git.NewOid(report.Hash)
		if err != nil {
			ctx.Log.Error(
				"failed to parse commit",
				map[string]interface{}{
					"filename": reportPath,
					"commit":   report.Hash,
					"err":      err,
				},
			)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		commit, err := repository.LookupCommit(commitID)
		if err != nil {
			ctx.Log.Error(
				"failed to lookup commit",
				map[string]interface{}{
					"filename": reportPath,
					"commit":   report.Hash,
					"err":      err,
				},
			)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer commit.Free()
	}

	ctx.Metrics.CounterAdd("grader_ci_jobs_total", 1)

	if err := os.MkdirAll(path.Dir(reportPath), 0755); err != nil {
		ctx.Log.Error(
			"Failed to create the report directory",
			map[string]interface{}{
				"filename": reportPath,
				"err":      err,
			},
		)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	stamp, err := os.OpenFile(
		path.Join(path.Dir(reportPath), ci.RunningStampFilename),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0644,
	)
	if err != nil {
		ctx.Log.Error(
			"Failed to create the running stamp",
			map[string]interface{}{
				"filename": reportPath,
				"err":      err,
			},
		)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	stamp.Close()

	if err := report.Write(reportPath); err != nil {
		ctx.Log.Error(
			"Failed to create the report file",
			map[string]interface{}{
				"filename": reportPath,
				"err":      err,
			},
		)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(report); err != nil {
		ctx.Log.Error(
			"Failed to write report",
			map[string]interface{}{
				"err": err,
			},
		)
	}

	// Transfer the run to processCIRequest.
	h.requestChan <- &startedCIRequest{
		report:       report,
		path:         reportPath,
		problemFiles: problemFiles,
	}
}

func (h *ciHandler) runTest(
	ctx *grader.Context,
	testConfig *ci.TestConfig,
	runs *grader.Queue,
	report *ci.Report,
	reportPath string,
) error {
	testConfig.Test.StartTime = time.Now()

	ctx.Metrics.CounterAdd("grader_ephemeral_runs_total", 1)
	ctx.Log.Debug(
		"Adding new run",
		map[string]interface{}{
			"run": &grader.EphemeralRunRequest{
				Source:   testConfig.Solution.Source,
				Language: testConfig.Solution.Language,
				Input:    testConfig.Input,
			},
		},
	)
	maxScore := &big.Rat{}
	for _, literalCase := range testConfig.Input.Cases {
		maxScore.Add(maxScore, literalCase.Weight)
	}
	inputFactory, err := common.NewLiteralInputFactory(
		testConfig.Input,
		ctx.Config.Grader.RuntimePath,
		common.LiteralPersistGrader,
	)
	if err != nil {
		ctx.Log.Error(
			"Error creating input factory",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}

	runInfo := grader.NewRunInfo()
	runInfo.Run.InputHash = inputFactory.Hash()
	runInfo.Run.MaxScore = maxScore
	runInfo.Run.Language = testConfig.Solution.Language
	runInfo.Run.Source = testConfig.Solution.Source
	runInfo.Priority = grader.QueuePriorityEphemeral
	testConfig.Test.EphemeralToken, err = h.ephemeralRunManager.SetEphemeral(runInfo)
	if err != nil {
		ctx.Log.Error(
			"Error making run ephemeral",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}

	committed := false
	defer func(committed *bool) {
		if *committed {
			return
		}
		err = runInfo.Artifacts.Clean()
		if err != nil {
			ctx.Log.Error(
				"Error cleaning up after run",
				map[string]interface{}{
					"err": err,
				},
			)
		}
	}(&committed)

	inputRef, err := ctx.InputManager.Add(inputFactory.Hash(), inputFactory)
	if err != nil {
		ctx.Log.Error(
			"Error adding input",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}
	runWaitHandle, err := runs.AddWaitableRun(&ctx.Context, runInfo, inputRef)
	if err != nil {
		ctx.Log.Error(
			"Failed to add run",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}

	ctx.Log.Info(
		"enqueued run",
		map[string]interface{}{
			"run": runInfo.Run,
		},
	)

	// Wait until a runner has picked the run up, or the run has been finished.
	select {
	case <-runWaitHandle.Running():
		testConfig.Test.State = ci.StateRunning
		if err := report.Write(reportPath); err != nil {
			ctx.Log.Error(
				"Failed to write the report file",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
		}
		break
	case <-runWaitHandle.Ready():
	}
	<-runWaitHandle.Ready()

	{
		finishTime := time.Now()
		testConfig.Test.FinishTime = &finishTime
		duration := base.Duration(testConfig.Test.FinishTime.Sub(testConfig.Test.StartTime))
		testConfig.Test.Duration = &duration
	}
	testConfig.Test.SetResult(&runInfo.Result)

	if err := report.Write(reportPath); err != nil {
		ctx.Log.Error(
			"Failed to write the report file",
			map[string]interface{}{
				"filename": reportPath,
				"err":      err,
			},
		)
	}

	// Finally commit the run to the manager.
	if err = saveEphemeralRunRequest(
		ctx,
		runInfo,
		&grader.EphemeralRunRequest{
			Source:   testConfig.Solution.Source,
			Language: testConfig.Solution.Language,
			Input:    testConfig.Input,
		},
	); err != nil {
		ctx.Log.Error(
			"Failed to commit the original request",
			map[string]interface{}{
				"err": err,
			},
		)

		return err
	}
	h.ephemeralRunManager.Commit(runInfo)
	committed = true
	ctx.Log.Info(
		"Finished running ephemeral run",
		map[string]interface{}{
			"token": testConfig.Test.EphemeralToken,
		},
	)

	return nil
}

func (h *ciHandler) processCIRequest(
	report *ci.Report,
	reportPath string,
	problemFiles *common.ProblemFiles,
	runs *grader.Queue,
) {
	ctx := h.ctx.Wrap(context.TODO())
	ctx.Log.Info(
		"running request",
		map[string]interface{}{
			"report": report,
		},
	)

	var err error

	if report.IsEphemeral {
		if problemFiles == nil {
			err = fmt.Errorf("Missing problem files for ephemeral run")
		}
	} else {
		p, tmpE := common.NewProblemFilesFromGit(
			grader.GetRepositoryPath(
				ctx.Config.Grader.RuntimePath,
				report.Problem,
			),
			report.Hash,
		)
		problemFiles = &p
		err = tmpE
	}

	if err != nil {
		ctx.Log.Error(
			"Failed to validate problem files",
			map[string]interface{}{
				"err": err,
			},
		)
		report.State = ci.StateError
		report.ReportError = &ci.ReportError{Error: err}
		{
			finishTime := time.Now()
			report.FinishTime = &finishTime
			duration := base.Duration(report.FinishTime.Sub(report.StartTime))
			report.Duration = &duration
		}
		if err := report.Write(reportPath); err != nil {
			ctx.Log.Error(
				"Failed to write the report file",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
		}
		return
	}
	ciRunConfig, err := ci.NewRunConfig(*problemFiles, false)
	if err != nil {
		ctx.Log.Error(
			"Failed to validate commit",
			map[string]interface{}{
				"err": err,
			},
		)
		if base.HasErrorCategory(err, ci.ErrSkipped) {
			report.State = ci.StateSkipped
		} else {
			report.State = ci.StateError
		}
		report.ReportError = &ci.ReportError{Error: err}
		{
			finishTime := time.Now()
			report.FinishTime = &finishTime
			duration := base.Duration(report.FinishTime.Sub(report.StartTime))
			report.Duration = &duration
		}
		if err := report.Write(reportPath); err != nil {
			ctx.Log.Error(
				"Failed to write the report file",
				map[string]interface{}{
					"filename": reportPath,
					"err":      err,
				},
			)
		}
		return
	}

	// We can get rid of this reference now: the in-memory data structures will
	// get persisted to disk (and freed) as the runs are enqueued.
	problemFiles = nil

	for _, testConfig := range ciRunConfig.TestConfigs {
		report.Tests = append(report.Tests, testConfig.Test)
	}
	if err := report.Write(reportPath); err != nil {
		ctx.Log.Error(
			"Failed to write the report file",
			map[string]interface{}{
				"filename": reportPath,
				"err":      err,
			},
		)
	}

	report.State = ci.StateRunning
	for _, testConfig := range ciRunConfig.TestConfigs {
		if err := h.runTest(ctx, testConfig, runs, report, reportPath); err != nil {
			ctx.Log.Error(
				"Failed to perform ephemeral run",
				map[string]interface{}{
					"err": err,
				},
			)
			testConfig.Test.State = ci.StateError
			testConfig.Test.ReportError = &ci.ReportError{Error: err}
		}
	}
	report.State = ci.StatePassed
	report.UpdateState()

	{
		finishTime := time.Now()
		report.FinishTime = &finishTime
		duration := base.Duration(report.FinishTime.Sub(report.StartTime))
		report.Duration = &duration
	}
	ctx.Log.Info(
		"running request",
		map[string]interface{}{
			"report": report,
		},
	)
	if err := report.Write(reportPath); err != nil {
		ctx.Log.Error(
			"Failed to write the report file",
			map[string]interface{}{
				"filename": reportPath,
				"err":      err,
			},
		)
	}

	ctx.Log.Info(
		"finished running request",
		map[string]interface{}{
			"report": report,
		},
	)

	if err := os.Remove(path.Join(path.Dir(reportPath), ci.RunningStampFilename)); err != nil {
		ctx.Log.Error(
			"Failed to remove the running stamp",
			map[string]interface{}{
				"filename": reportPath,
				"err":      err,
			},
		)
	}

	h.lruCache.AddRun(
		path.Dir(reportPath),
		fmt.Sprintf("%s/%s", report.Problem, report.Hash),
	)
}

func (h *ciHandler) run() {
	ctx := h.ctx.Wrap(context.TODO())
	runs, err := ctx.QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}

	ciRoot := path.Join(ctx.Config.Grader.RuntimePath, "ci")
	ctx.Log.Info("Reloading CI runs...", nil)
	if err := h.lruCache.ReloadRuns(ciRoot); err != nil {
		ctx.Log.Error(
			"Reloading CI runs failed",
			map[string]interface{}{
				"err": err,
			},
		)
	}
	ctx.Log.Info(
		"Finished preloading CI runs",
		map[string]interface{}{
			"cache_size": h.lruCache.Size(),
		},
	)

	ctx.Log.Info("CI run manager ready", nil)
	for {
		select {
		case <-h.stopChan:
			close(h.doneChan)
			return

		case request := <-h.requestChan:
			h.processCIRequest(request.report, request.path, request.problemFiles, runs)
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
		lruCache:            ci.NewLRUCache(ctx.Config.Grader.CI.CISizeLimit, ctx.Log),
		stopChan:            make(chan struct{}),
		requestChan:         make(chan *startedCIRequest, 128),
		doneChan:            make(chan struct{}),
	}
	mux.Handle(ctx.Tracing.WrapHandle("/ci/", ciHandler))
	go ciHandler.run()
	return ciHandler
}
