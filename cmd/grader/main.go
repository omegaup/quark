package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/lhchavez/quark/grader"
	"github.com/lhchavez/quark/runner"
	"golang.org/x/net/http2"
	"html"
	"io"
	"io/ioutil"
	"net/http"
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
	http2.ConfigureServer(server, &http2.Server{})

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
	expvar.Publish("codemanager_size", expvar.Func(func() interface{} {
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
		run, err := runs.AddRun(ctx, id, ctx.InputManager)
		if err != nil {
			ctx.Log.Error(err.Error(), "id", id)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		ctx.Log.Info("enqueued run", "run", run)
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	http.HandleFunc("/run/request/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := context()
		runnerName := PeerName(r)
		ctx.Log.Debug("requesting run", "proto", r.Proto, "client", runnerName)

		timeout := make(chan bool)
		runCtx, ok := runs.GetRun(
			runnerName,
			ctx.InflightMonitor,
			w.(http.CloseNotifier).CloseNotify(),
			timeout,
		)
		if !ok {
			ctx.Log.Debug("client gone", "client", runnerName)
		} else {
			go func() {
				if <-timeout {
					ctx.Log.Error("run timed out. retrying", "context", runCtx)
					if !runCtx.Requeue() {
						ctx.Log.Error("run timed out too many times. giving up")
					}
				}
				close(timeout)
			}()
			ctx.Log.Debug("served run", "run", runCtx.Run, "client", runnerName)
			w.Header().Set("Content-Type", "text/json; charset=utf-8")
			encoder := json.NewEncoder(w)
			encoder.Encode(runCtx.Run)
			runCtx.Input.Release()
		}
	})

	runRe := regexp.MustCompile("/run/([0-9]+)/(results|files)/?")
	http.HandleFunc("/run/", func(w http.ResponseWriter, r *http.Request) {
		ctx := context()
		defer r.Body.Close()
		res := runRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		runID, _ := strconv.ParseUint(res[1], 10, 64)
		runCtx, ok := ctx.InflightMonitor.Get(runID)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		uploadType := res[2]
		gradeDir := path.Join(
			ctx.Config.Grader.RuntimePath,
			"grade",
			fmt.Sprintf("%02d", runCtx.ID%10),
			fmt.Sprintf("%d", runCtx.ID),
		)
		if err := os.MkdirAll(gradeDir, 0755); err != nil {
			ctx.Log.Error("Unable to create grade dir", "err", err)
			return
		}
		if uploadType == "results" {
			defer ctx.InflightMonitor.Remove(runID)
			var result runner.RunResult
			decoder := json.NewDecoder(r.Body)
			if err := decoder.Decode(&result); err != nil {
				ctx.Log.Error("Error obtaining result", "err", err)
			} else {
				resultsPath := path.Join(gradeDir, "details.json")
				fd, err := os.Create(resultsPath)
				if err != nil {
					ctx.Log.Error("Unable to create results file", "err", err)
					return
				}
				defer fd.Close()
				prettyPrinted, err := json.MarshalIndent(&result, "", "  ")
				if err != nil {
					ctx.Log.Error("Unable to marshal results file", "err", err)
					return
				}
				if _, err := fd.Write(prettyPrinted); err != nil {
					ctx.Log.Error("Unable to write results file", "err", err)
					return
				}
				ctx.Log.Info("Results ready for run", "ctx", runCtx)
			}
		} else {
			filesPath := path.Join(gradeDir, "files.zip")
			fd, err := os.Create(filesPath)
			if err != nil {
				ctx.Log.Error("Unable to create results file", "err", err)
				return
			}
			if _, err := io.Copy(fd, r.Body); err != nil {
				ctx.Log.Error("Unable to upload results", "err", err)
			}
			ctx.Log.Info("Files ready for run", "ctx", runCtx)
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
		defer input.Release()
		if err := input.Transmit(w); err != nil {
			ctx.Log.Error("Error transmitting input", "hash", hash, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	if err := startServer(ctx); err != nil {
		panic(err)
	}
}
