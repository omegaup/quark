package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/omegaup/quark/context"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/queue"
	"golang.org/x/net/http2"
	"html"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
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

type RunContext struct {
	Run   *queue.Run
	Input context.Input
}

func NewRunContext(run *queue.Run, ctx *context.Context) (*RunContext, error) {
	input, err := context.DefaultInputManager.Get(run.InputHash,
		grader.NewGraderInputFactory(run, &ctx.Config))
	if err != nil {
		return nil, err
	}

	runctx := &RunContext{
		Run:   run,
		Input: input,
	}
	return runctx, nil
}

func loadContext() error {
	ctx, err := context.NewContext(*configPath)
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func startServer(ctx *context.Context) error {
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", ctx.Config.Grader.Port),
	}
	http2.ConfigureServer(server, &http2.Server{})

	ctx.Log.Info("omegaUp grader started")
	if *insecure {
		return server.ListenAndServe()
	} else {
		cert, err := ioutil.ReadFile(ctx.Config.Grader.CertFile)
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

		return server.ListenAndServeTLS(ctx.Config.Grader.CertFile,
			ctx.Config.Grader.KeyFile)
	}
}

func main() {
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	expvar.Publish("config", &globalContext.Load().(*context.Context).Config)

	var runs = make(chan *RunContext,
		globalContext.Load().(*context.Context).Config.Grader.ChannelLength)
	context.InitInputManager(globalContext.Load().(*context.Context))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %q %q", html.EscapeString(r.URL.Path), r.TLS.PeerCertificates[0].Subject.CommonName)
	})
	gradeRe := regexp.MustCompile("/run/grade/(\\d+)/?")
	http.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
		ctx := globalContext.Load().(*context.Context)
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
		run, err := queue.NewRun(id, ctx)
		if err != nil {
			ctx.Log.Error(err.Error(), "id", id)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		runCtx, err := NewRunContext(run, ctx)
		if err != nil {
			ctx.Log.Error(err.Error(), "id", id)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		runs <- runCtx
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		ctx.Log.Info("enqueued run", "run", run)
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})
	http.HandleFunc("/run/request", func(w http.ResponseWriter, r *http.Request) {
		run := <-runs
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		encoder := json.NewEncoder(w)
		encoder.Encode(run)
		run.Input.Release()
	})

	if err := startServer(globalContext.Load().(*context.Context)); err != nil {
		panic(err)
	}
}
