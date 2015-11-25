package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/grader"
	"github.com/lhchavez/quark/runner"
	"golang.org/x/net/http2"
	"html"
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
	configPath = flag.String("config", "/etc/omegaup/grader/config.json",
		"Grader configuration file")
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

		return server.ListenAndServeTLS(ctx.Config.TLS.CertFile,
			ctx.Config.TLS.KeyFile)
	}
}

func main() {
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := globalContext.Load().(*grader.Context)
	expvar.Publish("config", &ctx.Config)

	var runs = grader.NewQueue("default", ctx.Config.Grader.ChannelLength)
	inputManager := common.NewInputManager(&ctx.Context)
	expvar.Publish("codemanager_size", expvar.Func(func() interface{} {
		return inputManager.Size()
	}))
	cachePath := path.Join(ctx.Config.Grader.RuntimePath, "cache")
	go inputManager.PreloadInputs(
		cachePath,
		grader.NewGraderCachedInputFactory(cachePath),
		&sync.Mutex{},
	)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %q %q", html.EscapeString(r.URL.Path), r.TLS.PeerCertificates[0].Subject.CommonName)
	})
	gradeRe := regexp.MustCompile("/run/grade/(\\d+)/?")
	http.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
		ctx := globalContext.Load().(*grader.Context)
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
		runCtx, err := grader.NewRunContext(ctx, id, inputManager)
		if err != nil {
			ctx.Log.Error(err.Error(), "id", id)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		runs.Enqueue(runCtx, 1)
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		ctx.Log.Info("enqueued run", "run", runCtx.Run)
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	http.HandleFunc("/run/request/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := globalContext.Load().(*grader.Context)
		ctx.Log.Debug("requesting run",
			"proto", r.Proto, "client", r.TLS.PeerCertificates[0].Subject.CommonName)

		timeout := make(chan bool)
		runCtx, ok := runs.GetRun(w.(http.CloseNotifier).CloseNotify(), timeout)
		if !ok {
			ctx.Log.Debug("client gone",
				"client", r.TLS.PeerCertificates[0].Subject.CommonName)
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
			ctx.Log.Debug("served run", "run", runCtx.Run,
				"client", r.TLS.PeerCertificates[0].Subject.CommonName)
			w.Header().Set("Content-Type", "text/json; charset=utf-8")
			encoder := json.NewEncoder(w)
			encoder.Encode(runCtx.Run)
			runCtx.Input.Release()
		}
	})

	runRe := regexp.MustCompile("/run/([0-9]+)/(results|files)/?")
	http.HandleFunc("/run/", func(w http.ResponseWriter, r *http.Request) {
		ctx := globalContext.Load().(*grader.Context)
		defer r.Body.Close()
		res := runRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		runID, _ := strconv.ParseUint(res[1], 10, 64)
		uploadType := res[2]
		if uploadType == "results" {
			runCtx := grader.GlobalInflightMonitor.Get(runID)
			var result runner.RunResult
			decoder := json.NewDecoder(r.Body)
			if err := decoder.Decode(&result); err != nil {
				ctx.Log.Error("Error obtaining result", "err", err)
			} else {
				defer grader.GlobalInflightMonitor.Remove(runID)
				resultsPath := path.Join(ctx.Config.Grader.RuntimePath, "grade",
					runCtx.Run.GUID[:2], runCtx.Run.GUID[2:], "details.json")
				if err := os.MkdirAll(path.Dir(resultsPath), 0755); err != nil {
					ctx.Log.Error("Unable to create dir", "err", err)
					return
				}
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
			}
		} else {
			ctx.Log.Info("handled", "id", runID, "type", uploadType)
		}
	})

	inputRe := regexp.MustCompile("/input/([a-f0-9]{40})/?")
	http.HandleFunc("/input/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ctx := globalContext.Load().(*grader.Context)
		res := inputRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hash := res[1]
		input, err := inputManager.Get(hash)
		if err != nil {
			ctx.Log.Error("Input not found", "hash", hash)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer input.Release()
		if err := input.(*grader.GraderInput).Transmit(w); err != nil {
			ctx.Log.Error("Error transmitting input", "hash", hash, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	if err := startServer(ctx); err != nil {
		panic(err)
	}
}
