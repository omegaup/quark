package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/elazarl/go-bindata-assetfs"
	"github.com/lhchavez/quark/broadcaster"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/grader"
	"github.com/lhchavez/quark/grader/v1compat"
	"github.com/lhchavez/quark/runner"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/http2"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"time"
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
	GUIDs   []string `json:"id"`
	Rejudge bool     `json:"rejudge"`
	Debug   bool     `json:"debug"`
}

func v1CompatUpdateDatabase(
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
			run.Result.Memory,
			run.Result.Score,
			run.Result.ContestScore,
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
			run.Result.Memory,
			run.Result.Score,
			run.Result.ContestScore,
			run.Result.JudgedBy,
			run.ID,
		)
		if err != nil {
			ctx.Log.Error("Error updating the database", "err", err, "run", run)
		}
	}
}

func v1CompatWriteResults(
	ctx *grader.Context,
	run *grader.RunInfo,
) {
	f, err := os.Create(path.Join(run.GradeDir, "results.json"))
	if err != nil {
		ctx.Log.Error("Error creating results.json", "err", err, "run", run)
		return
	}
	defer f.Close()
	result := struct {
		ID          int64
		GUID        string
		Contest     *string
		ProblemName string
		Result      *runner.RunResult
	}{
		ID:          run.ID,
		GUID:        run.GUID,
		Contest:     run.Contest,
		ProblemName: run.ProblemName,
		Result:      &run.Result,
	}
	marshaled, err := json.MarshalIndent(&result, "", " ")
	if err != nil {
		ctx.Log.Error("Error marshaling results", "err", err, "run", run)
		return
	}
	if _, err = f.Write(marshaled); err != nil {
		ctx.Log.Error("Error writing results.json", "err", err, "run", run)
	}
}

func v1CompatBroadcastRun(
	ctx *grader.Context,
	db *sql.DB,
	client *http.Client,
	run *grader.RunInfo,
) error {
	message := broadcaster.Message{
		Problem: run.ProblemName,
		Public:  false,
	}
	if run.Contest != nil {
		message.Contest = *run.Contest
	}
	type serializedRun struct {
		User         string  `json:"username"`
		Contest      *string `json:"contest_alias,omitempty"`
		Problem      string  `json:"alias"`
		GUID         string  `json:"guid"`
		Runtime      float64 `json:"runtime"`
		Penalty      float64 `json:"penalty"`
		Memory       int64   `json:"memory"`
		Score        float64 `json:"score"`
		ContestScore float64 `json:"contest_score"`
		Status       string  `json:"status"`
		Verdict      string  `json:"verdict"`
		SubmitDelay  float64 `json:"submit_delay"`
		Time         float64 `json:"time"`
		Language     string  `json:"language"`
	}
	type runFinishedMessage struct {
		Message string        `json:"message"`
		Run     serializedRun `json:"run"`
	}
	msg := runFinishedMessage{
		Message: "/run/update/",
		Run: serializedRun{
			Contest:      run.Contest,
			Problem:      message.Problem,
			GUID:         run.GUID,
			Runtime:      run.Result.Time,
			Memory:       run.Result.Memory,
			Score:        run.Result.Score,
			ContestScore: run.Result.ContestScore,
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
			u.username, r.penalty, r.submit_delay, UNIX_TIMESTAMP(r.time)
		FROM
			Runs r
		INNER JOIN
			Users u ON u.user_id = r.user_id
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

	marshaled, err := json.Marshal(&msg)
	if err != nil {
		return err
	}

	message.Message = string(marshaled)

	if err := v1CompatBroadcast(ctx, client, &message); err != nil {
		ctx.Log.Error("Error sending run broadcast", "err", err)
	}
	return nil
}

func v1CompatRunPostProcessor(
	db *sql.DB,
	finishedRuns <-chan *grader.RunInfo,
	client *http.Client,
) {
	ctx := context()
	for run := range finishedRuns {
		ctx.Metrics.GaugeAdd("grader_queue_total_length", -1)
		delay := time.Now().Sub(run.CreationTime).Seconds()
		ctx.Metrics.SummaryObserve("grader_queue_delay_seconds", delay)
		if run.Priority == grader.QueuePriorityLow {
			ctx.Metrics.SummaryObserve("grader_queue_low_delay_seconds", delay)
		} else if run.Priority == grader.QueuePriorityNormal {
			ctx.Metrics.SummaryObserve("grader_queue_normal_delay_seconds", delay)
		} else if run.Priority == grader.QueuePriorityHigh {
			ctx.Metrics.SummaryObserve("grader_queue_high_delay_seconds", delay)
		}
		if run.Result.Verdict == "JE" {
			ctx.Metrics.CounterAdd("grader_runs_je", 1)
		}
		if ctx.Config.Grader.V1.UpdateDatabase {
			v1CompatUpdateDatabase(ctx, db, run)
		}
		if ctx.Config.Grader.V1.WriteResults {
			v1CompatWriteResults(ctx, run)
		}
		if ctx.Config.Grader.V1.SendBroadcast {
			if err := v1CompatBroadcastRun(ctx, db, client, run); err != nil {
				ctx.Log.Error("Error sending run broadcast", "err", err)
			}
		}
	}
}

func v1CompatGetPendingRuns(ctx *grader.Context, db *sql.DB) ([]string, error) {
	rows, err := db.Query(
		`SELECT
			guid
		FROM
			Runs
		WHERE
			status != 'ready';`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	guids := make([]string, 0)
	for rows.Next() {
		var guid string
		err = rows.Scan(&guid)
		if err != nil {
			return nil, err
		}
		guids = append(guids, guid)
	}
	return guids, nil
}

func v1CompatNewRunContext(
	ctx *grader.Context,
	db *sql.DB,
	guid string,
) (*grader.RunContext, string, *common.ProblemSettings, error) {
	runCtx := grader.NewEmptyRunContext(ctx)
	runCtx.GUID = guid
	runCtx.GradeDir = path.Join(
		ctx.Config.Grader.V1.RuntimeGradePath,
		guid[:2],
		guid[2:],
	)
	var contestName sql.NullString
	var penaltyType sql.NullString
	var contestPoints sql.NullFloat64
	validatorLimits := common.DefaultValidatorLimits
	settings := common.ProblemSettings{}
	err := db.QueryRow(
		`SELECT
			r.run_id, c.alias, c.penalty_type, r.language, p.alias, pp.points,
			p.extra_wall_time, p.memory_limit, p.output_limit,
			p.overall_wall_time_limit, p.time_limit, p.validator_time_limit, p.slow,
			p.validator
		FROM
			Runs r
		INNER JOIN
			Problems p ON p.problem_id = r.problem_id
		LEFT JOIN
			Problemset_Problems pp ON pp.problem_id = r.problem_id AND
			pp.problemset_id = r.problemset_id
		LEFT JOIN
			Contests c ON c.problemset_id = pp.problemset_id
		WHERE
			r.guid = ?;`, guid).Scan(
		&runCtx.ID,
		&contestName,
		&penaltyType,
		&runCtx.Run.Language,
		&runCtx.ProblemName,
		&contestPoints,
		&settings.Limits.ExtraWallTime,
		&settings.Limits.MemoryLimit,
		&settings.Limits.OutputLimit,
		&settings.Limits.OverallWallTimeLimit,
		&settings.Limits.TimeLimit,
		&validatorLimits.TimeLimit,
		&settings.Slow,
		&settings.Validator.Name,
	)
	if err != nil {
		return nil, "", nil, err
	}

	settings.Limits.MemoryLimit *= 1024

	if settings.Validator.Name == "custom" {
		if validatorLimits.ExtraWallTime < settings.Limits.ExtraWallTime {
			validatorLimits.ExtraWallTime = settings.Limits.ExtraWallTime
		}
		if validatorLimits.MemoryLimit < settings.Limits.MemoryLimit {
			validatorLimits.MemoryLimit = settings.Limits.MemoryLimit
		}
		if validatorLimits.OutputLimit < settings.Limits.OutputLimit {
			validatorLimits.OutputLimit = settings.Limits.OutputLimit
		}
		if validatorLimits.OverallWallTimeLimit < settings.Limits.OverallWallTimeLimit {
			validatorLimits.OverallWallTimeLimit = settings.Limits.OverallWallTimeLimit
		}
		settings.Validator.Limits = &validatorLimits
	}

	gitProblemInfo, err := v1compat.GetProblemInformation(path.Join(
		ctx.Config.Grader.V1.RuntimePath,
		"problems.git",
		runCtx.ProblemName,
	))
	if err != nil {
		return nil, "", nil, err
	}

	if contestName.Valid {
		runCtx.Contest = &contestName.String
	}
	if penaltyType.Valid {
		runCtx.PenaltyType = penaltyType.String
	}
	if contestPoints.Valid {
		runCtx.Run.MaxScore = contestPoints.Float64
	} else {
		runCtx.Run.MaxScore = 1.0
	}
	runCtx.Result.MaxScore = runCtx.Run.MaxScore
	contents, err := ioutil.ReadFile(
		path.Join(
			ctx.Config.Grader.V1.RuntimePath,
			"submissions",
			runCtx.GUID[:2],
			runCtx.GUID[2:],
		),
	)
	if err != nil {
		return nil, "", nil, err
	}
	runCtx.Run.Source = string(contents)

	runCtx.Run.InputHash = v1compat.VersionedHash(ctx.LibinteractiveVersion, gitProblemInfo, &settings)
	return runCtx, gitProblemInfo.TreeID, &settings, nil
}

func v1CompatInjectRuns(
	ctx *grader.Context,
	runs *grader.Queue,
	db *sql.DB,
	guids []string,
	priority grader.QueuePriority,
) error {
	for _, guid := range guids {
		runCtx, gitTree, settings, err := v1CompatNewRunContext(ctx, db, guid)
		if err != nil {
			ctx.Log.Error(
				"Error getting run context",
				"err", err,
				"guid", guid,
			)
			return err
		}
		if settings.Slow {
			runCtx.Priority = grader.QueuePriorityLow
		} else {
			runCtx.Priority = priority
		}
		ctx.Log.Info("RunContext", "runCtx", runCtx)
		ctx.Metrics.GaugeAdd("grader_queue_total_length", 1)
		ctx.Metrics.CounterAdd("grader_runs_total", 1)
		input, err := ctx.InputManager.Add(
			runCtx.Run.InputHash,
			v1compat.NewInputFactory(
				runCtx.ProblemName,
				&ctx.Config,
				&v1compat.SettingsLoader{
					Settings: settings,
					GitTree:  gitTree,
				},
			),
		)
		if err != nil {
			ctx.Log.Error("Error getting input", "err", err, "run", runCtx)
			return err
		}
		if err = grader.AddRunContext(ctx, runCtx, input); err != nil {
			ctx.Log.Error("Error adding run context", "err", err, "guid", guid)
			return err
		}
		runs.AddRun(runCtx)
	}
	return nil
}

func v1CompatBroadcast(
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

func registerV1CompatHandlers(mux *http.ServeMux, db *sql.DB) {
	runs, err := context().QueueManager.Get("default")
	if err != nil {
		panic(err)
	}
	guids, err := v1CompatGetPendingRuns(context(), db)
	if err != nil {
		panic(err)
	}
	for _, guid := range guids {
		if err := v1CompatInjectRuns(
			context(),
			runs,
			db,
			[]string{guid},
			grader.QueuePriorityNormal,
		); err != nil {
			context().Log.Error("Error injecting run", "guid", guid, "err", err)
		}
	}
	context().Log.Info("Injected pending runs", "count", len(guids))

	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if !*insecure {
		cert, err := ioutil.ReadFile(context().Config.TLS.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)
		keyPair, err := tls.LoadX509KeyPair(
			context().Config.TLS.CertFile,
			context().Config.TLS.KeyFile,
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
	context().InflightMonitor.PostProcessor.AddListener(finishedRunsChan)
	go v1CompatRunPostProcessor(db, finishedRunsChan, client)

	mux.Handle("/", http.FileServer(&wrappedFileSystem{
		fileSystem: &assetfs.AssetFS{
			Asset:     Asset,
			AssetDir:  AssetDir,
			AssetInfo: AssetInfo,
			Prefix:    "data",
		},
	}))

	mux.Handle("/metrics", prometheus.Handler())

	mux.HandleFunc("/grader/status/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
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

	mux.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
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
		if err = v1CompatInjectRuns(ctx, runs, db, request.GUIDs, priority); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	mux.HandleFunc("/run/payload/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var request runGradeRequest
		if err := decoder.Decode(&request); err != nil {
			ctx.Log.Error("Error receiving grade request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Info("/run/payload/", "request", request)
		var response = make(map[string]*common.Run)
		for _, guid := range request.GUIDs {
			runCtx, _, _, err := v1CompatNewRunContext(ctx, db, guid)
			if err != nil {
				ctx.Log.Error(
					"Error getting run context",
					"err", err,
					"guid", guid,
				)
				response[guid] = nil
			} else {
				response[guid] = runCtx.Run
			}
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)
	})

	mux.HandleFunc("/broadcast/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var message broadcaster.Message
		if err := decoder.Decode(&message); err != nil {
			ctx.Log.Error("Error receiving broadcast request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Info("/broadcast/", "message", message)
		if err := v1CompatBroadcast(ctx, client, &message); err != nil {
			ctx.Log.Error("Error sending broadcast message", "err", err)
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	mux.HandleFunc("/reload-config/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
		ctx.Log.Info("/reload-config/")
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})
}
