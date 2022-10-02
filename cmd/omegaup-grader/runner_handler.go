package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
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
	// Best-effort deletion of the grade dir.
	runCtx.RunInfo.Artifacts.Clean()
	runCtx.RunInfo.Result.JudgedBy = runnerName

	multipartReader, err := r.MultipartReader()
	if err != nil {
		runCtx.Log.Error(
			"Error decoding multipart data",
			map[string]any{
				"err":    err,
				"runner": runnerName,
			},
		)
		return &processRunStatus{http.StatusBadRequest, true}
	}
	for {
		part, err := multipartReader.NextPart()
		if err == io.EOF {
			break
		} else if err != nil {
			runCtx.Log.Error(
				"Error receiving next file",
				map[string]any{
					"err":    err,
					"runner": runnerName,
				},
			)
			return &processRunStatus{http.StatusBadRequest, true}
		}
		runCtx.Log.Debug(
			"Processing file",
			map[string]any{
				"attempt_id": attemptID,
				"filename":   part.FileName(),
				"runner":     runnerName,
			},
		)

		if part.FileName() == ".keepalive" {
			// Do nothing, this is only here to keep the connection alive.
		} else if part.FileName() == "details.json" {
			var result runner.RunResult
			decoder := json.NewDecoder(part)
			decoder.UseNumber()
			if err := decoder.Decode(&result); err != nil {
				runCtx.Log.Error(
					"Error obtaining result",
					map[string]any{
						"err":    err,
						"runner": runnerName,
					},
				)
				return &processRunStatus{http.StatusBadRequest, true}
			}
			runCtx.RunInfo.Result = result
			runCtx.RunInfo.Result.JudgedBy = runnerName
		} else if part.FileName() == "logs.txt" {
			var buffer bytes.Buffer
			if _, err := io.Copy(&buffer, part); err != nil {
				runCtx.Log.Error(
					"Unable to read logs",
					map[string]any{
						"err":    err,
						"runner": runnerName,
					},
				)
				return &processRunStatus{http.StatusBadRequest, true}
			}
			runCtx.AppendLogSection(runnerName, buffer.Bytes())
		} else {
			err = runCtx.RunInfo.Artifacts.Put(runCtx.Context, part.FileName(), part)
			if err != nil {
				runCtx.Log.Error(
					"Unable to upload results",
					map[string]any{
						"err":    err,
						"runner": runnerName,
					},
				)
				return &processRunStatus{http.StatusBadRequest, true}
			}
		}
	}
	runCtx.Log.Info(
		"Finished processing run",
		map[string]any{
			"verdict": runCtx.RunInfo.Result.Verdict,
			"score":   runCtx.RunInfo.Result.Score,
			"runner":  runnerName,
			"runInfo": runCtx.RunInfo,
		},
	)
	if runCtx.RunInfo.Result.Verdict == "JE" {
		// Retry the run in case it is some transient problem.
		runCtx.Log.Info(
			"Judge Error. Re-attempting run.",
			map[string]any{
				"verdict": runCtx.RunInfo.Result.Verdict,
				"runner":  runnerName,
				"runInfo": runCtx.RunInfo,
			},
		)
		return &processRunStatus{http.StatusOK, true}
	}
	return &processRunStatus{http.StatusOK, false}
}

func registerRunnerHandlers(
	ctx *grader.Context,
	mux *http.ServeMux,
	db *sql.DB,
	insecure bool,
) {
	runs, err := ctx.QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}

	mux.Handle(ctx.Tracing.WrapHandle("/monitoring/benchmark/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx = ctx.Wrap(r.Context())
		defer r.Body.Close()
		runnerName := peerName(r, insecure)
		f, err := os.OpenFile("benchmark.txt", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)
		if err != nil {
			ctx.Log.Error(
				"Failed to open benchmark file",
				map[string]any{
					"err": err,
				},
			)
			return
		}
		defer f.Close()
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("%d %s ", time.Now().Unix(), runnerName))
		io.Copy(&buf, r.Body)
		buf.WriteString("\n")
		if _, err := io.Copy(f, &buf); err != nil {
			ctx.Log.Error(
				"Failed to write to benchmark file",
				map[string]any{
					"err": err,
				},
			)
		}
	})))

	mux.Handle(ctx.Tracing.WrapHandle("/run/request/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx = ctx.Wrap(r.Context())
		defer r.Body.Close()
		runnerName := peerName(r, insecure)
		ctx.Log.Debug(
			"requesting run",
			map[string]any{
				"proto":  r.Proto,
				"client": runnerName,
			},
		)

		// Add the runner to the list of known runners.
		m, ok := ctx.Metrics.(*prometheusMetrics)
		if ok {
			remoteAddr := r.Header.Get("OmegaUp-Runner-PublicIP")
			if remoteAddr != "" {
				m.RunnerObserve(remoteAddr + ":6060")
			}
		}

		runCtx, _, ok := runs.GetRun(
			runnerName,
			ctx.InflightMonitor,
			w.(http.CloseNotifier).CloseNotify(),
		)
		if !ok {
			ctx.Log.Debug(
				"client gone",
				map[string]any{
					"client": runnerName,
				},
			)
			return
		}

		runCtx.Log.Debug(
			"served run",
			map[string]any{
				"run":    runCtx,
				"client": runnerName,
			},
		)
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		// TODO: Remove this.
		w.Header().Set("Sync-ID", "0")
		encoder := json.NewEncoder(w)
		runCtx.Transaction.InsertDistributedTraceHeaders(w.Header())
		encoder.Encode(runCtx.RunInfo.Run)
	})))

	runRe := regexp.MustCompile("/run/([0-9]+)/results/?")
	mux.Handle(ctx.Tracing.WrapHandle("/run/", http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx = ctx.Wrap(r.Context())
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
			return
		}
		runCtx.Log.Error(
			"run errored out. retrying",
			map[string]any{
				"context": runCtx,
			},
		)
		// status is OK only when the runner successfully sent a JE verdict.
		lastAttempt := result.status == http.StatusOK
		runCtx.Requeue(lastAttempt)
	}), time.Duration(5*time.Minute), "Request timed out")))

	inputRe := regexp.MustCompile("/input/(?:([a-zA-Z0-9_-]*)/)?([a-f0-9]{40})/?")
	mux.Handle(ctx.Tracing.WrapHandle("/input/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx = ctx.Wrap(r.Context())
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
			inputRef, err = ctx.InputManager.Add(
				hash,
				&common.CacheOnlyInputFactoryForTesting{},
			)
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
			ctx.Log.Error(
				"Input not found",
				map[string]any{
					"hash": hash,
				},
			)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer inputRef.Release()
		if err := inputRef.Input.(common.TransmittableInput).Transmit(w); err != nil {
			ctx.Log.Error(
				"Error transmitting input",
				map[string]any{
					"hash": hash,
					"err":  err,
				},
			)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})))
}
