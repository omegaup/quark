package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/broadcaster"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/http2"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	guidRegex = regexp.MustCompile("^[0-9a-f]{32}$")
)

type graderRunningStatus struct {
	RunnerName string `json:"name"`
	ID         int64  `json:"id"`
}

type graderStatusQueue struct {
	Running           []graderRunningStatus `json:"running"`
	RunQueueLength    int                   `json:"run_queue_length"`
	RunnerQueueLength int                   `json:"runner_queue_length"`
	Runners           []string              `json:"runners"`
}

type graderStatusResponse struct {
	Status            string            `json:"status"`
	BoadcasterSockets int               `json:"broadcaster_sockets"`
	EmbeddedRunner    bool              `json:"embedded_runner"`
	RunningQueue      graderStatusQueue `json:"queue"`
}

type runGradeRequest struct {
	RunIDs  []int64 `json:"run_ids,omitempty"`
	Rejudge bool    `json:"rejudge"`
	Debug   bool    `json:"debug"`
}

type runGradeResource struct {
	RunID    int64  `json:"run_id,omitempty"`
	Filename string `json:"filename"`
}

func updateDatabase(
	ctx *grader.Context,
	db *sql.DB,
	run *grader.RunInfo,
) {
	if run.PenaltyType == "runtime" {
		_, err := db.Exec(
			`UPDATE
				Runs
			SET
				status = 'ready', verdict = ?, runtime = ?, penalty = ?, memory = ?,
				score = ?, contest_score = ?, judged_by = ?
			WHERE
				run_id = ?;`,
			run.Result.Verdict,
			run.Result.Time*1000,
			run.Result.Time*1000,
			run.Result.Memory.Bytes(),
			common.RationalToFloat(run.Result.Score),
			common.RationalToFloat(run.Result.ContestScore),
			run.Result.JudgedBy,
			run.ID,
		)
		if err != nil {
			ctx.Log.Error("Error updating the database", "err", err, "run", run)
		}
	} else {
		_, err := db.Exec(
			`UPDATE
				Runs
			SET
				status = 'ready', verdict = ?, runtime = ?, memory = ?, score = ?,
				contest_score = ?, judged_by = ?
			WHERE
				run_id = ?;`,
			run.Result.Verdict,
			run.Result.Time*1000,
			run.Result.Memory.Bytes(),
			common.RationalToFloat(run.Result.Score),
			common.RationalToFloat(run.Result.ContestScore),
			run.Result.JudgedBy,
			run.ID,
		)
		if err != nil {
			ctx.Log.Error("Error updating the database", "err", err, "run", run)
		}
	}
}

func broadcastRun(
	ctx *grader.Context,
	db *sql.DB,
	client *http.Client,
	run *grader.RunInfo,
) error {
	message := broadcaster.Message{
		Problem: run.ProblemName,
		Public:  false,
	}
	if run.ID == 0 {
		// Ephemeral run. No need to broadcast.
		return nil
	}
	if run.Contest != nil {
		message.Contest = *run.Contest
	}
	type serializedRun struct {
		User         string    `json:"username"`
		Contest      *string   `json:"contest_alias,omitempty"`
		Problemset   *int64    `json:"problemset,omitempty"`
		Problem      string    `json:"alias"`
		GUID         string    `json:"guid"`
		Runtime      float64   `json:"runtime"`
		Penalty      float64   `json:"penalty"`
		Memory       base.Byte `json:"memory"`
		Score        float64   `json:"score"`
		ContestScore float64   `json:"contest_score"`
		Status       string    `json:"status"`
		Verdict      string    `json:"verdict"`
		SubmitDelay  float64   `json:"submit_delay"`
		Time         float64   `json:"time"`
		Language     string    `json:"language"`
	}
	type runFinishedMessage struct {
		Message string        `json:"message"`
		Run     serializedRun `json:"run"`
	}
	msg := runFinishedMessage{
		Message: "/run/update/",
		Run: serializedRun{
			Contest:      run.Contest,
			Problemset:   run.Problemset,
			Problem:      run.ProblemName,
			GUID:         run.GUID,
			Runtime:      run.Result.Time,
			Memory:       run.Result.Memory,
			Score:        common.RationalToFloat(run.Result.Score),
			ContestScore: common.RationalToFloat(run.Result.ContestScore),
			Status:       "ready",
			Verdict:      run.Result.Verdict,
			Language:     run.Run.Language,
			Time:         -1,
			SubmitDelay:  -1,
			Penalty:      -1,
		},
	}

	err := db.QueryRow(
		`SELECT
			u.username, r.penalty, s.submit_delay, UNIX_TIMESTAMP(r.time)
		FROM
			Runs r
		INNER JOIN
			Submissions s ON s.submission_id = r.submission_id
		INNER JOIN
			Users u ON u.main_identity_id = s.identity_id
		WHERE
			r.run_id = ?;`, run.ID).Scan(
		&msg.Run.User,
		&msg.Run.Penalty,
		&msg.Run.SubmitDelay,
		&msg.Run.Time,
	)
	if err != nil {
		return err
	}
	message.User = msg.Run.User
	if run.Problemset != nil {
		message.Problemset = *run.Problemset
	}

	marshaled, err := json.Marshal(&msg)
	if err != nil {
		return err
	}

	message.Message = string(marshaled)

	if err := broadcast(ctx, client, &message); err != nil {
		ctx.Log.Error("Error sending run broadcast", "err", err)
	}
	return nil
}

func runPostProcessor(
	db *sql.DB,
	finishedRuns <-chan *grader.RunInfo,
	client *http.Client,
) {
	ctx := graderContext()
	for run := range finishedRuns {
		if run.Result.Verdict == "JE" {
			ctx.Metrics.CounterAdd("grader_runs_je", 1)
		}
		if ctx.Config.Grader.V1.UpdateDatabase {
			updateDatabase(ctx, db, run)
		}
		if ctx.Config.Grader.V1.SendBroadcast {
			if err := broadcastRun(ctx, db, client, run); err != nil {
				ctx.Log.Error("Error sending run broadcast", "err", err)
			}
		}
	}
}

func getPendingRuns(ctx *grader.Context, db *sql.DB) ([]int64, error) {
	rows, err := db.Query(
		`SELECT
			run_id
		FROM
			Runs
		WHERE
			status != 'ready';`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runIds []int64
	for rows.Next() {
		var runId int64
		err = rows.Scan(&runId)
		if err != nil {
			return nil, err
		}
		runIds = append(runIds, runId)
	}
	return runIds, nil
}

// gradeDir gets the new-style Run ID-based path.
func gradeDir(ctx *grader.Context, runID int64) string {
	return path.Join(
		ctx.Config.Grader.V1.RuntimeGradePath,
		fmt.Sprintf("%02d/%02d/%d", runID%100, (runID%10000)/100, runID),
	)
}

func newRunContext(
	ctx *grader.Context,
	runCtx *grader.RunContext,
) (*grader.RunContext, error) {
	runCtx.GradeDir = gradeDir(ctx, runCtx.ID)
	gitProblemInfo, err := grader.GetProblemInformation(grader.GetRepositoryPath(
		ctx.Config.Grader.V1.RuntimePath,
		runCtx.ProblemName,
	))
	if err != nil {
		return nil, err
	}

	runCtx.Result.MaxScore = runCtx.Run.MaxScore

	if gitProblemInfo.Settings.Slow {
		runCtx.Priority = grader.QueuePriorityLow
	} else {
		runCtx.Priority = grader.QueuePriorityNormal
	}

	return runCtx, nil
}

func newRunContextFromID(
	ctx *grader.Context,
	db *sql.DB,
	runId int64,
) (*grader.RunContext, error) {
	runCtx := grader.NewEmptyRunContext(ctx)
	runCtx.ID = runId
	var contestName sql.NullString
	var problemset sql.NullInt64
	var penaltyType sql.NullString
	var contestPoints sql.NullFloat64
	err := db.QueryRow(
		`SELECT
			s.guid, c.alias, s.problemset_id, c.penalty_type, s.language,
			p.alias, pp.points, r.version
		FROM
			Runs r
		INNER JOIN
			Submissions s ON s.submission_id = r.submission_id
		INNER JOIN
			Problems p ON p.problem_id = s.problem_id
		LEFT JOIN
			Problemset_Problems pp ON pp.problem_id = s.problem_id AND
			pp.problemset_id = s.problemset_id
		LEFT JOIN
			Contests c ON c.problemset_id = pp.problemset_id
		WHERE
			r.run_id = ?;`, runCtx.ID).Scan(
		&runCtx.GUID,
		&contestName,
		&problemset,
		&penaltyType,
		&runCtx.Run.Language,
		&runCtx.ProblemName,
		&contestPoints,
		&runCtx.Run.InputHash,
	)
	if err != nil {
		return nil, err
	}

	if contestName.Valid {
		runCtx.Contest = &contestName.String
	}
	if problemset.Valid {
		runCtx.Problemset = &problemset.Int64
	}
	if penaltyType.Valid {
		runCtx.PenaltyType = penaltyType.String
	}
	if contestPoints.Valid {
		runCtx.Run.MaxScore = common.FloatToRational(contestPoints.Float64)
	} else {
		runCtx.Run.MaxScore = big.NewRat(1, 1)
	}
	return newRunContext(ctx, runCtx)
}

func readSource(ctx *grader.Context, runCtx *grader.RunContext) error {
	contents, err := ioutil.ReadFile(
		path.Join(
			ctx.Config.Grader.V1.RuntimePath,
			"submissions",
			runCtx.GUID[:2],
			runCtx.GUID[2:],
		),
	)
	if err != nil {
		return err
	}
	runCtx.Run.Source = string(contents)
	return nil
}

func injectRuns(
	ctx *grader.Context,
	runs *grader.Queue,
	priority grader.QueuePriority,
	runCtxs ...*grader.RunContext,
) error {
	for _, runCtx := range runCtxs {
		if err := readSource(ctx, runCtx); err != nil {
			ctx.Log.Error(
				"Error getting run source",
				"err", err,
				"runId", runCtx.ID,
			)
			return err
		}
		if runCtx.Priority == grader.QueuePriorityNormal {
			runCtx.Priority = priority
		}
		ctx.Log.Info("RunContext", "runCtx", runCtx)
		ctx.Metrics.CounterAdd("grader_runs_total", 1)
		input, err := ctx.InputManager.Add(
			runCtx.Run.InputHash,
			grader.NewInputFactory(
				runCtx.ProblemName,
				&ctx.Config,
			),
		)
		if err != nil {
			ctx.Log.Error("Error getting input", "err", err, "run", runCtx)
			return err
		}
		if err = grader.AddRunContext(ctx, runCtx, input); err != nil {
			ctx.Log.Error("Error adding run context", "err", err, "runId", runCtx.ID)
			return err
		}
		runs.AddRun(runCtx)
	}
	return nil
}

func broadcast(
	ctx *grader.Context,
	client *http.Client,
	message *broadcaster.Message,
) error {
	marshaled, err := json.Marshal(message)
	if err != nil {
		return err
	}

	resp, err := client.Post(
		ctx.Config.Grader.BroadcasterURL,
		"text/json",
		bytes.NewReader(marshaled),
	)
	ctx.Log.Debug("Broadcast", "message", message, "resp", resp, "err", err)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf(
			"Request to broadcast failed with error code %d",
			resp.StatusCode,
		)
	}
	return nil
}

func registerFrontendHandlers(mux *http.ServeMux, db *sql.DB) {
	runs, err := graderContext().QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}
	runIds, err := getPendingRuns(graderContext(), db)
	if err != nil {
		panic(err)
	}
	// Don't block while the runs are being injected. This prevents potential
	// deadlocks where there are more runs than what the queue can hold, and the
	// queue cannot be drained unless the transport is connected.
	go func() {
		graderContext().Log.Info("Injecting pending runs", "count", len(runIds))
		for _, runId := range runIds {
			runCtx, err := newRunContextFromID(graderContext(), db, runId)
			if err != nil {
				graderContext().Log.Error(
					"Error getting run context",
					"err", err,
					"runId", runId,
				)
				continue
			}
			if err := injectRuns(
				graderContext(),
				runs,
				grader.QueuePriorityNormal,
				runCtx,
			); err != nil {
				graderContext().Log.Error("Error injecting run", "runId", runId, "err", err)
			}
		}
		graderContext().Log.Info("Injected pending runs", "count", len(runIds))
	}()

	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if !*insecure {
		cert, err := ioutil.ReadFile(graderContext().Config.TLS.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)
		keyPair, err := tls.LoadX509KeyPair(
			graderContext().Config.TLS.CertFile,
			graderContext().Config.TLS.KeyFile,
		)
		transport.TLSClientConfig = &tls.Config{
			Certificates: []tls.Certificate{keyPair},
			RootCAs:      certPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}
		if err != nil {
			panic(err)
		}
		if err := http2.ConfigureTransport(transport); err != nil {
			panic(err)
		}
	}

	client := &http.Client{Transport: transport}

	finishedRunsChan := make(chan *grader.RunInfo, 1)
	graderContext().InflightMonitor.PostProcessor.AddListener(finishedRunsChan)
	go runPostProcessor(db, finishedRunsChan, client)

	mux.Handle("/metrics", prometheus.Handler())

	mux.HandleFunc("/grader/status/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		runData := ctx.InflightMonitor.GetRunData()
		status := graderStatusResponse{
			Status: "ok",
			RunningQueue: graderStatusQueue{
				Runners: []string{},
				Running: make([]graderRunningStatus, len(runData)),
			},
		}

		for i, data := range runData {
			status.RunningQueue.Running[i].RunnerName = data.Runner
			status.RunningQueue.Running[i].ID = data.ID
		}
		for _, queueInfo := range ctx.QueueManager.GetQueueInfo() {
			for _, l := range queueInfo.Lengths {
				status.RunningQueue.RunQueueLength += l
			}
		}
		encoder := json.NewEncoder(w)
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		if err := encoder.Encode(&status); err != nil {
			ctx.Log.Error("Error writing /grader/status/ response", "err", err)
		}
	})

	mux.HandleFunc("/run/new/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()

		if r.Method != "POST" {
			ctx.Log.Error("Invalid request", "url", r.URL.Path, "method", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		tokens := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

		if len(tokens) != 3 {
			ctx.Log.Error("Invalid request", "url", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var runCtx *grader.RunContext
		runID, err := strconv.ParseUint(tokens[2], 10, 64)
		if err != nil {
			ctx.Log.Error("Invalid Run ID", "run id", tokens[2])
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if runCtx, err = newRunContextFromID(ctx, db, int64(runID)); err != nil {
			ctx.Log.Error(
				"/run/new/",
				"runID", runID,
				"response", "internal server error",
				"err", err,
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		filePath := path.Join(
			ctx.Config.Grader.V1.RuntimePath,
			"submissions",
			runCtx.GUID[:2],
			runCtx.GUID[2:],
		)
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
		if err != nil {
			if os.IsExist(err) {
				ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "already exists")
				w.WriteHeader(http.StatusConflict)
				return
			}
			ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer f.Close()

		io.Copy(f, r.Body)

		if err = injectRuns(ctx, runs, grader.QueuePriorityNormal, runCtx); err != nil {
			ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "ok")
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var request runGradeRequest
		if err := decoder.Decode(&request); err != nil {
			ctx.Log.Error("Error receiving grade request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Info("/run/grade/", "request", request)
		priority := grader.QueuePriorityNormal
		if request.Rejudge || request.Debug {
			priority = grader.QueuePriorityLow
		}
		var runCtxs []*grader.RunContext
		for _, runID := range request.RunIDs {
			runCtx, err := newRunContextFromID(ctx, db, runID)
			if err != nil {
				graderContext().Log.Error(
					"Error getting run context",
					"err", err,
					"run id", runID,
				)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			runCtxs = append(runCtxs, runCtx)
		}
		if err = injectRuns(ctx, runs, priority, runCtxs...); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	mux.HandleFunc("/submission/source/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()

		if r.Method != "GET" {
			ctx.Log.Error("Invalid request", "url", r.URL.Path, "method", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		tokens := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

		if len(tokens) != 3 {
			ctx.Log.Error("Invalid request", "url", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		guid := tokens[2]

		if len(guid) != 32 || !guidRegex.MatchString(guid) {
			ctx.Log.Error("Invalid GUID", "guid", guid)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		filePath := path.Join(
			ctx.Config.Grader.V1.RuntimePath,
			"submissions",
			guid[:2],
			guid[2:],
		)
		f, err := os.Open(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				ctx.Log.Info("/run/source/", "guid", guid, "response", "not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}
			ctx.Log.Info("/run/source/", "guid", guid, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			ctx.Log.Info("/run/source/", "guid", guid, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))

		ctx.Log.Info("/run/source/", "guid", guid, "response", "ok")
		w.WriteHeader(http.StatusOK)
		io.Copy(w, f)
	})

	mux.HandleFunc("/run/resource/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var request runGradeResource
		if err := decoder.Decode(&request); err != nil {
			ctx.Log.Error("Error receiving resource request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if request.RunID == 0 {
			ctx.Log.Info("/run/resource/", "request", request, "response", "not found", "err", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if request.Filename == "" || strings.HasPrefix(request.Filename, ".") ||
			strings.Contains(request.Filename, "/") {
			ctx.Log.Error("Invalid filename", "filename", request.Filename)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		filePath := path.Join(
			gradeDir(ctx, request.RunID),
			request.Filename,
		)
		f, err := os.Open(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				ctx.Log.Info("/run/resource/", "request", request, "response", "not found", "err", err)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			ctx.Log.Info("/run/resource/", "request", request, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			ctx.Log.Info("/run/resource/", "request", request, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))

		ctx.Log.Info("/run/resource/", "request", request, "response", "ok")
		w.WriteHeader(http.StatusOK)
		io.Copy(w, f)
	})

	mux.HandleFunc("/broadcast/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var message broadcaster.Message
		if err := decoder.Decode(&message); err != nil {
			ctx.Log.Error("Error receiving broadcast request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Debug("/broadcast/", "message", message)
		if err := broadcast(ctx, client, &message); err != nil {
			ctx.Log.Error("Error sending broadcast message", "err", err)
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	mux.HandleFunc("/reload-config/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		ctx.Log.Info("/reload-config/")
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})
}
