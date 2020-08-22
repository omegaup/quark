package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path"
	"regexp"
	"time"

	git "github.com/lhchavez/git2go/v29"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner/ci"
)

var (
	ciURLRegexp = regexp.MustCompile(`^/ci/problem/([a-zA-Z0-9-_]+)/([0-9a-f]{40})/$`)
)

type reportWithPath struct {
	report *ci.Report
	path   string
}

type ciHandler struct {
	ephemeralRunManager *grader.EphemeralRunManager
	ctx                 *grader.Context
	lruCache            *ci.LRUCache
	stopChan            chan struct{}
	reportChan          chan *reportWithPath
	doneChan            chan struct{}
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

	report := &ci.Report{
		Problem:    match[1],
		CommitHash: match[2],
		StartTime:  time.Now(),
		State:      ci.StateWaiting,
	}

	reportPath := path.Join(
		h.ctx.Config.Grader.RuntimePath,
		"ci",
		report.Problem,
		report.CommitHash[:2],
		report.CommitHash[2:],
		"report.json.gz",
	)
	if fd, err := os.Open(reportPath); err == nil {
		defer fd.Close()

		st, err := fd.Stat()
		if err != nil {
			h.ctx.Log.Error("Failed to stat the file", "filename", reportPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Add("Content-Type", "application/json")
		w.Header().Add("Content-Encoding", "gzip")
		http.ServeContent(w, r, reportPath, st.ModTime(), fd)
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
			"filename", reportPath,
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
			"filename", reportPath,
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
			"filename", reportPath,
			"commit", report.CommitHash,
			"err", err,
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer commit.Free()

	h.ctx.Metrics.CounterAdd("grader_ci_jobs_total", 1)

	if err := os.MkdirAll(path.Dir(reportPath), 0755); err != nil {
		h.ctx.Log.Error("Failed to create the report directory", "filename", reportPath, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	stamp, err := os.OpenFile(
		path.Join(path.Dir(reportPath), ci.RunningStampFilename),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0644,
	)
	if err != nil {
		h.ctx.Log.Error("Failed to create the running stamp", "filename", reportPath, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	stamp.Close()

	if err := report.Write(reportPath); err != nil {
		h.ctx.Log.Error("Failed to create the report file", "filename", reportPath, "err", err)
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

	// Transfer the run to processCIRequest.
	h.reportChan <- &reportWithPath{
		report: report,
		path:   reportPath,
	}
}

func (h *ciHandler) runTest(
	testConfig *ci.TestConfig,
	runs *grader.Queue,
	report *ci.Report,
	reportPath string,
) error {
	testConfig.Test.StartTime = time.Now()

	h.ctx.Metrics.CounterAdd("grader_ephemeral_runs_total", 1)
	h.ctx.Log.Debug(
		"Adding new run",
		"run", &grader.EphemeralRunRequest{
			Source:   testConfig.Source,
			Language: testConfig.Language,
			Input:    testConfig.Input,
		},
	)
	maxScore := &big.Rat{}
	for _, literalCase := range testConfig.Input.Cases {
		maxScore.Add(maxScore, literalCase.Weight)
	}
	inputFactory, err := common.NewLiteralInputFactory(
		testConfig.Input,
		h.ctx.Config.Grader.RuntimePath,
		common.LiteralPersistGrader,
	)
	if err != nil {
		h.ctx.Log.Error("Error creating input factory", "err", err)
		return err
	}

	runInfo := grader.NewRunInfo()
	runInfo.Run.InputHash = inputFactory.Hash()
	runInfo.Run.Language = testConfig.Language
	runInfo.Run.MaxScore = maxScore
	runInfo.Run.Source = testConfig.Source
	runInfo.Priority = grader.QueuePriorityEphemeral
	testConfig.Test.EphemeralToken, err = h.ephemeralRunManager.SetEphemeral(runInfo)
	if err != nil {
		h.ctx.Log.Error("Error making run ephemeral", "err", err)
		return err
	}

	committed := false
	defer func(committed *bool) {
		if *committed {
			return
		}
		if err := os.RemoveAll(runInfo.GradeDir); err != nil {
			h.ctx.Log.Error("Error cleaning up after run", "err", err)
		}
	}(&committed)

	inputRef, err := h.ctx.InputManager.Add(inputFactory.Hash(), inputFactory)
	if err != nil {
		h.ctx.Log.Error("Error adding input", "err", err)
		return err
	}
	runWaitHandle, err := runs.AddWaitableRun(&h.ctx.Context, runInfo, inputRef)
	if err != nil {
		h.ctx.Log.Error("Failed to add run", "err", err)
		return err
	}

	h.ctx.Log.Info("enqueued run", "run", runInfo.Run)

	// Wait until a runner has picked the run up, or the run has been finished.
	select {
	case <-runWaitHandle.Running():
		testConfig.Test.State = ci.StateRunning
		if err := report.Write(reportPath); err != nil {
			h.ctx.Log.Error("Failed to write the report file", "filename", reportPath, "err", err)
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
		h.ctx.Log.Error("Failed to write the report file", "filename", reportPath, "err", err)
	}

	// Finally commit the run to the manager.
	if err = saveEphemeralRunRequest(
		h.ctx,
		runInfo,
		&grader.EphemeralRunRequest{
			Source:   testConfig.Source,
			Language: testConfig.Language,
			Input:    testConfig.Input,
		},
	); err != nil {
		h.ctx.Log.Error("Failed to commit the original request", "err", err)
		return err
	}
	h.ephemeralRunManager.Commit(runInfo)
	committed = true
	h.ctx.Log.Info("Finished running ephemeral run", "token", testConfig.Test.EphemeralToken)

	return nil
}

func (h *ciHandler) processCIRequest(report *ci.Report, reportPath string, runs *grader.Queue) {
	h.ctx.Log.Info("running request", "report", report)
	problemFiles, err := common.NewProblemFilesFromGit(
		grader.GetRepositoryPath(
			h.ctx.Config.Grader.RuntimePath,
			report.Problem,
		),
		report.CommitHash,
	)
	if err != nil {
		h.ctx.Log.Error("Failed to validate commit", "err", err)
		report.State = ci.StateError
		report.Error = err
		{
			finishTime := time.Now()
			report.FinishTime = &finishTime
			duration := base.Duration(report.FinishTime.Sub(report.StartTime))
			report.Duration = &duration
		}
		if err := report.Write(reportPath); err != nil {
			h.ctx.Log.Error("Failed to write the report file", "filename", reportPath, "err", err)
		}
		return
	}
	ciRunConfig, err := ci.NewRunConfig(problemFiles)
	if err != nil {
		h.ctx.Log.Error("Failed to validate commit", "err", err)
		if base.HasErrorCategory(err, ci.ErrSkipped) {
			report.State = ci.StateSkipped
		} else {
			report.State = ci.StateError
		}
		report.Error = err
		{
			finishTime := time.Now()
			report.FinishTime = &finishTime
			duration := base.Duration(report.FinishTime.Sub(report.StartTime))
			report.Duration = &duration
		}
		if err := report.Write(reportPath); err != nil {
			h.ctx.Log.Error("Failed to write the report file", "filename", reportPath, "err", err)
		}
		return
	}
	for _, testConfig := range ciRunConfig.TestConfigs {
		report.Tests = append(report.Tests, testConfig.Test)
	}
	if err := report.Write(reportPath); err != nil {
		h.ctx.Log.Error("Failed to write the report file", "filename", reportPath, "err", err)
	}

	report.State = ci.StateRunning
	for _, testConfig := range ciRunConfig.TestConfigs {
		if err := h.runTest(testConfig, runs, report, reportPath); err != nil {
			h.ctx.Log.Error("Failed to perform ephemeral run", "err", err)
			testConfig.Test.State = ci.StateError
			testConfig.Test.Error = err
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
	h.ctx.Log.Info("running request", "report", report)
	if err := report.Write(reportPath); err != nil {
		h.ctx.Log.Error("Failed to write the report file", "filename", reportPath, "err", err)
	}

	h.ctx.Log.Info("finished running request", "report", report)

	if err := os.Remove(path.Join(path.Dir(reportPath), ci.RunningStampFilename)); err != nil {
		h.ctx.Log.Error("Failed to remove the running stamp", "filename", reportPath, "err", err)
	}

	h.lruCache.AddRun(
		path.Dir(reportPath),
		fmt.Sprintf("%s/%s", report.Problem, report.CommitHash),
	)
}

func (h *ciHandler) run() {
	runs, err := h.ctx.QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}

	ciRoot := path.Join(h.ctx.Config.Grader.RuntimePath, "ci")
	h.ctx.Log.Info("Reloading CI runs...")
	if err := h.lruCache.ReloadRuns(ciRoot); err != nil {
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
			h.processCIRequest(report.report, report.path, runs)
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
		reportChan:          make(chan *reportWithPath, 128),
		doneChan:            make(chan struct{}),
	}
	mux.Handle("/ci/", ciHandler)
	go ciHandler.run()
	return ciHandler
}
