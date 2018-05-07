package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/elazarl/go-bindata-assetfs"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"
)

func newRunContext(ctx *grader.Context, db *sql.DB, id int64) (*grader.RunContext, error) {
	runCtx := grader.NewEmptyRunContext(ctx)
	runCtx.ID = id
	runCtx.GradeDir = path.Join(
		ctx.Config.Grader.RuntimePath,
		"grade",
		fmt.Sprintf("%02d", id%100),
		fmt.Sprintf("%d", id),
	)
	var contestName sql.NullString
	var contestPoints sql.NullFloat64
	err := db.QueryRow(
		`SELECT
			s.guid, c.alias, s.language, p.alias, pv.hash, cp.points
		FROM
			Runs r
		INNER JOIN
			Submissions s ON r.submission_id = s.submission_id
		INNER JOIN
			Problems p ON p.problem_id = s.problem_id
		INNER JOIN
			Problem_Versions pv ON pv.version_id = r.version_id
		LEFT JOIN
			Contests c ON c.contest_id = s.contest_id
		LEFT JOIN
			Contest_Problems cp ON cp.problem_id = s.problem_id AND
			cp.contest_id = s.contest_id
		WHERE
			r.run_id = ?;`, id).Scan(
		&runCtx.GUID, &contestName, &runCtx.Run.Language, &runCtx.ProblemName,
		&runCtx.Run.InputHash, &contestPoints)
	if err != nil {
		return nil, err
	}
	if contestName.Valid {
		runCtx.Contest = &contestName.String
	}
	if contestPoints.Valid {
		runCtx.Run.MaxScore = common.FloatToRational(contestPoints.Float64)
	}
	runCtx.Result.MaxScore = runCtx.Run.MaxScore
	contents, err := ioutil.ReadFile(
		path.Join(
			ctx.Config.Grader.RuntimePath,
			"submissions",
			runCtx.GUID[:2],
			runCtx.GUID[2:],
		),
	)
	if err != nil {
		return nil, err
	}
	runCtx.Run.Source = string(contents)
	return runCtx, nil
}

func processRun(
	r *http.Request,
	attemptID uint64,
	runCtx *grader.RunContext,
) *processRunStatus {
	runnerName := peerName(r)
	// TODO: make this a per-attempt directory so we can only commit directories
	// that will be not retried.
	gradeDir := runCtx.GradeDir
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
			runCtx.Result = result
			runCtx.Result.JudgedBy = runnerName
		} else if part.FileName() == "logs.txt" {
			var buffer bytes.Buffer
			if _, err := io.Copy(&buffer, part); err != nil {
				runCtx.Log.Error("Unable to read logs", "err", err, "runner", runnerName)
				return &processRunStatus{http.StatusBadRequest, true}
			}
			runCtx.AppendRunnerLogs(runnerName, buffer.Bytes())
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
		"verdict", runCtx.Result.Verdict,
		"score", runCtx.Result.Score,
		"runner", runnerName,
		"ctx", runCtx,
	)
	if runCtx.Result.Verdict == "JE" {
		// Retry the run in case it is some transient problem.
		runCtx.Log.Info(
			"Judge Error. Re-attempting run.",
			"verdict", runCtx.Result.Verdict,
			"runner", runnerName,
			"ctx", runCtx,
		)
		return &processRunStatus{http.StatusOK, true}
	}
	return &processRunStatus{http.StatusOK, false}
}

func registerHandlers(mux *http.ServeMux, db *sql.DB) {
	runs, err := context().QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}

	mux.Handle("/", http.FileServer(&wrappedFileSystem{
		fileSystem: &assetfs.AssetFS{
			Asset:     Asset,
			AssetDir:  AssetDir,
			AssetInfo: AssetInfo,
			Prefix:    "data/dist/admin",
		},
	}))

	mux.HandleFunc("/monitoring/benchmark/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := context()
		runnerName := peerName(r)
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

	gradeRe := regexp.MustCompile("/run/grade/(\\d+)/?")
	mux.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
		res := gradeRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		id, err := strconv.ParseInt(res[1], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		runCtx, err := newRunContext(ctx, db, id)
		if err != nil {
			ctx.Log.Error("Error getting run context", "err", err, "id", id)
			if err == sql.ErrNoRows {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
		input, err := ctx.InputManager.Add(
			runCtx.Run.InputHash,
			grader.NewInputFactory(runCtx.ProblemName, &ctx.Config),
		)
		if err != nil {
			ctx.Log.Error("Error getting input", "err", err, "run", runCtx)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err = grader.AddRunContext(ctx, runCtx, input); err != nil {
			ctx.Log.Error("Error adding run context", "err", err, "id", id)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, ok := r.URL.Query()["debug"]; ok {
			runCtx.Priority = grader.QueuePriorityLow
			if err := runCtx.Debug(); err != nil {
				ctx.Log.Error("Unable to set debug mode", "err", err)
			} else {
				defer func() {
					if err := os.RemoveAll(runCtx.GradeDir); err != nil {
						ctx.Log.Error("Error writing response", "err", err)
					}
				}()
			}
		}
		runs.AddRun(runCtx)
		runCtx.Log.Info("enqueued run", "run", runCtx.Run)
		if _, ok := r.URL.Query()["wait"]; ok {
			select {
			case <-w.(http.CloseNotifier).CloseNotify():
				return
			case <-runCtx.Ready():
			}

			if _, ok := r.URL.Query()["multipart"]; ok {
				multipartWriter := multipart.NewWriter(w)
				defer multipartWriter.Close()

				w.Header().Set("Content-Type", multipartWriter.FormDataContentType())
				files := []string{"logs.txt.gz", "files.zip", "details.json", "tracing.json.gz"}
				for _, file := range files {
					fd, err := os.Open(path.Join(runCtx.GradeDir, file))
					if err != nil {
						ctx.Log.Error("Error opening file", "file", file, "err", err)
						continue
					}
					resultWriter, err := multipartWriter.CreateFormFile("file", file)
					if err != nil {
						ctx.Log.Error("Error sending file", "file", file, "err", err)
						continue
					}
					if _, err := io.Copy(resultWriter, fd); err != nil {
						ctx.Log.Error("Error sending file", "file", file, "err", err)
						continue
					}
				}
			} else {
				w.Header().Set("Content-Type", "text/json; charset=utf-8")

				jsonData, _ := json.MarshalIndent(runCtx.Result, "", "  ")
				logData, err := readGzippedFile(path.Join(runCtx.GradeDir, "logs.txt.gz"))
				if err != nil {
					ctx.Log.Error("Error reading logs", "err", err)
				}
				filesZip, err := readBase64File(path.Join(runCtx.GradeDir, "files.zip"))
				if err != nil {
					ctx.Log.Error("Error reading logs", "err", err)
				}
				tracing, err := readBase64File(path.Join(runCtx.GradeDir, "tracing.json.gz"))
				if err != nil {
					ctx.Log.Error("Error reading logs", "err", err)
				}
				response := &ResponseStruct{
					Results:  string(jsonData),
					Logs:     logData,
					FilesZip: filesZip,
					Tracing:  tracing,
				}
				encoder := json.NewEncoder(w)
				if err := encoder.Encode(response); err != nil {
					ctx.Log.Error("Error writing response", "err", err)
				}
			}
		} else {
			w.Header().Set("Content-Type", "text/json; charset=utf-8")
			fmt.Fprintf(w, "{\"status\":\"ok\"}")
		}
	})

	mux.HandleFunc("/run/request/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := context()
		runnerName := peerName(r)
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
			encoder.Encode(runCtx.Run)
			runCtx.EventCollector.Add(ev)
		}
	})

	runRe := regexp.MustCompile("/run/([0-9]+)/results/?")
	mux.Handle("/run/", http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
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
		result := processRun(r, attemptID, runCtx)
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

	inputRe := regexp.MustCompile("/input/([a-f0-9]{40})/?")
	mux.HandleFunc("/input/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := context()
		res := inputRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hash := res[1]
		input, err := ctx.InputManager.Get(hash)
		if err != nil {
			ctx.Log.Error("Input not found", "hash", hash)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer input.Release(input)
		if err := input.Transmit(w); err != nil {
			ctx.Log.Error("Error transmitting input", "hash", hash, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
}
