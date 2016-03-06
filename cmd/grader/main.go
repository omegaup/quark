package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/grader"
	"github.com/lhchavez/quark/runner"
	"html"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	standalone = flag.Bool("standalone", false, "Standalone mode")
	configPath = flag.String(
		"config",
		"/etc/omegaup/grader/config.json",
		"Grader configuration file",
	)
	globalContext atomic.Value
	server        *http.Server
)

type processRunStatus struct {
	status int
	retry  bool
}

func loadContext() error {
	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, err := grader.NewContext(f)
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func context() *grader.Context {
	return globalContext.Load().(*grader.Context)
}

func startServer(ctx *grader.Context) error {
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", ctx.Config.Grader.Port),
	}

	ctx.Log.Info("omegaUp grader started")
	if *insecure {
		return server.ListenAndServe()
	} else {
		cert, err := ioutil.ReadFile(ctx.Config.TLS.CertFile)
		if err != nil {
			return err
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)

		tlsConfig := &tls.Config{
			ClientCAs:  certPool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		}
		tlsConfig.BuildNameToCertificate()
		server.TLSConfig = tlsConfig

		return server.ListenAndServeTLS(
			ctx.Config.TLS.CertFile,
			ctx.Config.TLS.KeyFile,
		)
	}
}

func processRun(
	r *http.Request,
	attemptID uint64,
	runCtx *grader.RunContext,
) *processRunStatus {
	runnerName := PeerName(r)
	// TODO: make this a per-attempt directory so we can only commit directories
	// that will be not retried.
	gradeDir := runCtx.GradeDir()
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
			if err := decoder.Decode(&result); err != nil {
				runCtx.Log.Error("Error obtaining result", "err", err, "runner", runnerName)
				return &processRunStatus{http.StatusBadRequest, true}
			}
			runCtx.Result = result
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
			if _, err := io.Copy(fd, part); err != nil {
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

func PeerName(r *http.Request) string {
	if *insecure {
		return r.RemoteAddr
	} else {
		return r.TLS.PeerCertificates[0].Subject.CommonName
	}
}

func main() {
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := context()
	expvar.Publish("config", &ctx.Config)

	runs, err := ctx.QueueManager.Get("default")
	if err != nil {
		panic(err)
	}
	expvar.Publish("codemanager", expvar.Func(func() interface{} {
		return context().InputManager
	}))
	expvar.Publish("queues", expvar.Func(func() interface{} {
		return context().QueueManager
	}))
	expvar.Publish("inflight_runs", expvar.Func(func() interface{} {
		return context().InflightMonitor
	}))
	cachePath := path.Join(ctx.Config.Grader.RuntimePath, "cache")
	go ctx.InputManager.PreloadInputs(
		cachePath,
		grader.NewGraderCachedInputFactory(cachePath),
		&sync.Mutex{},
	)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(
			w,
			"Hello, %q %q",
			html.EscapeString(r.URL.Path),
			PeerName(r),
		)
	})

	gradeRe := regexp.MustCompile("/run/grade/(\\d+)/?")
	http.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
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
		runCtx, err := runs.AddRun(ctx, id, ctx.InputManager)
		if err != nil {
			ctx.Log.Error(err.Error(), "id", id)
			if err == sql.ErrNoRows {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		runCtx.Log.Info("enqueued run", "run", runCtx.Run)
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	http.HandleFunc("/run/request/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := context()
		runnerName := PeerName(r)
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
	http.HandleFunc("/run/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
		defer r.Body.Close()
		res := runRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		attemptID, _ := strconv.ParseUint(res[1], 10, 64)
		runCtx, timeout, ok := ctx.InflightMonitor.Get(attemptID)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		resultChan := make(chan *processRunStatus, 1)
		go func() {
			select {
			case _, timedout := <-timeout:
				if !timedout {
					return
				}
				runCtx.Log.Error("run timed out")
				resultChan <- &processRunStatus{http.StatusRequestTimeout, true}
			}
		}()
		go func() {
			resultChan <- processRun(r, attemptID, runCtx)
		}()
		result := <-resultChan
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
	})

	inputRe := regexp.MustCompile("/input/([a-f0-9]{40})/?")
	http.HandleFunc("/input/", func(w http.ResponseWriter, r *http.Request) {
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

	if err := startServer(ctx); err != nil {
		panic(err)
	}
}
