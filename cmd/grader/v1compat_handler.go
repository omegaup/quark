package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/elazarl/go-bindata-assetfs"
	"github.com/lhchavez/quark/grader"
	"github.com/lhchavez/quark/runner"
	git "github.com/libgit2/git2go"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"net/http"
	"os"
	"path"
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

type broadcastRequest struct {
	ContestAlias string `json:"contest"`
	Message      string `json:"message"`
	Broadcast    bool   `json:"broadcast"`
	TargetUserId int    `json:"targetUser"`
	UserOnly     bool   `json:"userOnly"`
}

func v1CompatUpdateReadyRun(
	ctx *grader.Context,
	db *sql.DB,
	runCtx *grader.RunContext,
) {
	select {
	case <-runCtx.Ready():
	}

	if ctx.Config.Grader.V1.UpdateDatabase {
		_, err := db.Exec(
			`UPDATE
			Runs
		SET
			status = 'ready', verdict = ?, runtime = ?, memory = ?, score = ?,
			contest_score = ?, judged_by = ?
		WHERE
			run_id = ?;`,
			runCtx.Result.Verdict,
			runCtx.Result.Time,
			runCtx.Result.Memory,
			runCtx.Result.Score,
			runCtx.Result.ContestScore,
			runCtx.Result.JudgedBy,
			runCtx.ID,
		)
		if err != nil {
			ctx.Log.Error("Error updating the database", "err", err, "runCtx", runCtx)
		}
	}
	if ctx.Config.Grader.V1.WriteResults {
		f, err := os.Create(path.Join(runCtx.GradeDir, "results.json"))
		if err != nil {
			ctx.Log.Error("Error creating results.json", "err", err, "runCtx", runCtx)
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
			ID:          runCtx.ID,
			GUID:        runCtx.GUID,
			Contest:     runCtx.Contest,
			ProblemName: runCtx.ProblemName,
			Result:      &runCtx.Result,
		}
		bytes, err := json.MarshalIndent(&result, "", " ")
		if err != nil {
			ctx.Log.Error("Error marshaling results", "err", err, "runCtx", runCtx)
			return
		}
		if _, err = f.Write(bytes); err != nil {
			ctx.Log.Error("Error writing results.json", "err", err, "runCtx", runCtx)
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

func v1CompatGetTreeId(repositoryPath string) (string, error) {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return "", err
	}
	defer repository.Free()
	headRef, err := repository.Head()
	if err != nil {
		return "", err
	}
	defer headRef.Free()
	headObject, err := headRef.Peel(git.ObjectCommit)
	if err != nil {
		return "", err
	}
	defer headObject.Free()
	headCommit, err := headObject.AsCommit()
	if err != nil {
		return "", err
	}
	defer headCommit.Free()
	return headCommit.TreeId().String(), nil
}

func v1CompatNewRunContext(
	ctx *grader.Context,
	db *sql.DB,
	guid string,
) (*grader.RunContext, error) {
	runCtx := grader.NewEmptyRunContext(ctx)
	runCtx.GUID = guid
	runCtx.GradeDir = path.Join(
		ctx.Config.Grader.V1.RuntimePath,
		"grade",
		guid[:2],
		guid[2:],
	)
	var contestName sql.NullString
	var contestPoints sql.NullFloat64
	err := db.QueryRow(
		`SELECT
			r.run_id, c.alias, r.language, p.alias, cp.points
		FROM
			Runs r
		INNER JOIN
			Problems p ON p.problem_id = r.problem_id
		LEFT JOIN
			Contests c ON c.contest_id = r.contest_id
		LEFT JOIN
			Contest_Problems cp ON cp.problem_id = r.problem_id AND
			cp.contest_id = r.contest_id
		WHERE
			r.guid = ?;`, guid).Scan(
		&runCtx.ID, &contestName, &runCtx.Run.Language, &runCtx.ProblemName,
		&contestPoints)
	if err != nil {
		return nil, err
	}

	runCtx.Run.InputHash, err = v1CompatGetTreeId(path.Join(
		ctx.Config.Grader.V1.RuntimePath,
		"problems.git",
		runCtx.ProblemName,
	))
	if err != nil {
		return nil, err
	}

	if contestName.Valid {
		runCtx.Contest = &contestName.String
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
		return nil, err
	}
	runCtx.Run.Source = string(contents)
	go v1CompatUpdateReadyRun(ctx, db, runCtx)
	return runCtx, nil
}

func v1CompatInjectRuns(
	ctx *grader.Context,
	runs *grader.Queue,
	db *sql.DB,
	guids []string,
) error {
	for _, guid := range guids {
		runCtx, err := v1CompatNewRunContext(ctx, db, guid)
		if err != nil {
			ctx.Log.Error(
				"Error getting run context",
				"err", err,
				"guid", guid,
			)
			return err
		}
		ctx.Log.Info("RunContext", "runCtx", runCtx)
		input, err := ctx.InputManager.Add(
			runCtx.Run.InputHash,
			v1CompatNewGraderInputFactory(runCtx.ProblemName, &ctx.Config, db),
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

func registerV1CompatHandlers(mux *http.ServeMux, db *sql.DB) {
	runs, err := context().QueueManager.Get("default")
	if err != nil {
		panic(err)
	}
	guids, err := v1CompatGetPendingRuns(context(), db)
	if err != nil {
		panic(err)
	}
	if err := v1CompatInjectRuns(context(), runs, db, guids); err != nil {
		panic(err)
	}
	context().Log.Info("Injected pending runs", "count", len(guids))

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
		if err = v1CompatInjectRuns(ctx, runs, db, request.GUIDs); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	mux.HandleFunc("/broadcast/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var request broadcastRequest
		if err := decoder.Decode(&request); err != nil {
			ctx.Log.Error("Error receiving broadcast request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Info("/broadcast/", "request", request)
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
