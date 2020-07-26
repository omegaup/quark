package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"
)

func processRun(
	r *http.Request,
	attemptID uint64,
	runCtx *grader.RunContext,
	insecure bool,
) *processRunStatus {
	runnerName := peerName(r, insecure)
	// TODO: make this a per-attempt directory so we can only commit directories
	// that will be not retried.
	gradeDir := runCtx.RunInfo.GradeDir
	// Best-effort deletion of the grade dir.
	os.RemoveAll(gradeDir)
	if err := os.MkdirAll(gradeDir, 0755); err != nil {
		runCtx.Log.Error("Unable to create grade dir", "err", err, "runner", runnerName)
		return &processRunStatus{http.StatusInternalServerError, false}
	}

	multipartReader, err := r.MultipartReader()
	if err != nil {
		runCtx.Log.Error(
			"Error decoding multipart data",
			"err", err,
			"runner", runnerName,
		)
		return &processRunStatus{http.StatusBadRequest, true}
	}
	for {
		part, err := multipartReader.NextPart()
		if err == io.EOF {
			break
		} else if err != nil {
			runCtx.Log.Error("Error receiving next file", "err", err, "runner", runnerName)
			return &processRunStatus{http.StatusBadRequest, true}
		}
		runCtx.Log.Debug(
			"Processing file",
			"attempt_id", attemptID,
			"filename", part.FileName(),
			"runner", runnerName,
		)

		if part.FileName() == "details.json" {
			var result runner.RunResult
			decoder := json.NewDecoder(part)
			decoder.UseNumber()
			if err := decoder.Decode(&result); err != nil {
				runCtx.Log.Error("Error obtaining result", "err", err, "runner", runnerName)
				return &processRunStatus{http.StatusBadRequest, true}
			}
			runCtx.RunInfo.Result = result
			runCtx.RunInfo.Result.JudgedBy = runnerName
		} else if part.FileName() == "logs.txt" {
			var buffer bytes.Buffer
			if _, err := io.Copy(&buffer, part); err != nil {
				runCtx.Log.Error("Unable to read logs", "err", err, "runner", runnerName)
				return &processRunStatus{http.StatusBadRequest, true}
			}
			runCtx.AppendLogSection(runnerName, buffer.Bytes())
		} else if part.FileName() == "tracing.json" {
			var runnerCollector common.MemoryEventCollector
			decoder := json.NewDecoder(part)
			if err := decoder.Decode(&runnerCollector); err != nil {
				runCtx.Log.Error(
					"Unable to decode the tracing events",
					"err", err,
					"runner", runnerName,
				)
				return &processRunStatus{http.StatusBadRequest, true}
			}
			for _, e := range runnerCollector.Events {
				if err := runCtx.EventCollector.Add(e); err != nil {
					runCtx.Log.Error(
						"Unable to add tracing data",
						"err", err,
						"runner", runnerName,
					)
					break
				}
			}
		} else {
			filePath := path.Join(gradeDir, part.FileName())
			var w io.Writer
			if runCtx.Config.Grader.WriteGradeFiles {
				fd, err := os.Create(filePath)
				if err != nil {
					runCtx.Log.Error(
						"Unable to create results file",
						"err", err,
						"runner", runnerName,
					)
					return &processRunStatus{http.StatusInternalServerError, false}
				}
				defer fd.Close()
				w = fd
			} else {
				w = ioutil.Discard
			}
			if _, err := io.Copy(w, part); err != nil {
				runCtx.Log.Error(
					"Unable to upload results",
					"err", err,
					"runner", runnerName,
				)
				return &processRunStatus{http.StatusBadRequest, true}
			}
		}
	}
	runCtx.Log.Info(
		"Finished processing run",
		"verdict", runCtx.RunInfo.Result.Verdict,
		"score", runCtx.RunInfo.Result.Score,
		"runner", runnerName,
		"runInfo", runCtx.RunInfo,
	)
	if runCtx.RunInfo.Result.Verdict == "JE" {
		// Retry the run in case it is some transient problem.
		runCtx.Log.Info(
			"Judge Error. Re-attempting run.",
			"verdict", runCtx.RunInfo.Result.Verdict,
			"runner", runnerName,
			"runInfo", runCtx.RunInfo,
		)
		return &processRunStatus{http.StatusOK, true}
	}
	return &processRunStatus{http.StatusOK, false}
}

func registerRunnerHandlers(ctx *grader.Context, mux *http.ServeMux, db *sql.DB, insecure bool) {
	runs, err := ctx.QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}

	mux.HandleFunc("/monitoring/benchmark/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		runnerName := peerName(r, insecure)
		f, err := os.OpenFile("benchmark.txt", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)
		if err != nil {
			ctx.Log.Error("Failed to open benchmark file", "err", err)
			return
		}
		defer f.Close()
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("%d %s ", time.Now().Unix(), runnerName))
		io.Copy(&buf, r.Body)
		buf.WriteString("\n")
		if _, err := io.Copy(f, &buf); err != nil {
			ctx.Log.Error("Failed to write to benchmark file", "err", err)
		}
	})

	mux.HandleFunc("/run/request/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		runnerName := peerName(r, insecure)
		ctx.Log.Debug("requesting run", "proto", r.Proto, "client", runnerName)

		runCtx, _, ok := runs.GetRun(
			runnerName,
			ctx.InflightMonitor,
			w.(http.CloseNotifier).CloseNotify(),
		)
		if !ok {
			ctx.Log.Debug("client gone", "client", runnerName)
		} else {
			runCtx.Log.Debug("served run", "run", runCtx, "client", runnerName)
			w.Header().Set("Content-Type", "text/json; charset=utf-8")
			ev := runCtx.EventFactory.NewIssuerClockSyncEvent()
			w.Header().Set("Sync-ID", strconv.FormatUint(ev.SyncID, 10))
			encoder := json.NewEncoder(w)
			encoder.Encode(runCtx.RunInfo.Run)
			runCtx.EventCollector.Add(ev)
		}
	})

	runRe := regexp.MustCompile("/run/([0-9]+)/results/?")
	mux.Handle("/run/", http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		res := runRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		attemptID, _ := strconv.ParseUint(res[1], 10, 64)
		runCtx, _, ok := ctx.InflightMonitor.Get(attemptID)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		result := processRun(r, attemptID, runCtx, insecure)
		w.WriteHeader(result.status)
		if !result.retry {
			// The run either finished correctly or encountered a fatal error.
			// Close the context and write the results to disk.
			runCtx.Close()
		} else {
			runCtx.Log.Error("run errored out. retrying", "context", runCtx)
			// status is OK only when the runner successfully sent a JE verdict.
			lastAttempt := result.status == http.StatusOK
			if !runCtx.Requeue(lastAttempt) {
				runCtx.Log.Error("run errored out too many times. giving up")
			}
		}
	}), time.Duration(5*time.Minute), "Request timed out"))

	inputRe := regexp.MustCompile("/input/(?:([a-zA-Z0-9_-]*)/)?([a-f0-9]{40})/?")
	mux.HandleFunc("/input/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		res := inputRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		problemName := res[1]
		hash := res[2]
		var inputRef *common.InputRef
		if problemName == "" {
			inputRef, err = ctx.InputManager.Add(hash, &common.CacheOnlyInputFactoryForTesting{})
		} else {
			inputRef, err = ctx.InputManager.Add(
				hash,
				grader.NewInputFactory(
					problemName,
					&ctx.Config,
				),
			)
		}
		if err != nil {
			ctx.Log.Error("Input not found", "hash", hash)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer inputRef.Release()
		if err := inputRef.Input.(common.TransmittableInput).Transmit(w); err != nil {
			ctx.Log.Error("Error transmitting input", "hash", hash, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
}
