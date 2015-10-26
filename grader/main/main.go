package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
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

func loadContext() error {
	ctx, err := common.NewContext(*configPath)
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func startServer(ctx *common.Context) error {
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

	expvar.Publish("config", &globalContext.Load().(*common.Context).Config)

	var runs = make(chan *grader.RunContext,
		globalContext.Load().(*common.Context).Config.Grader.ChannelLength)
	common.InitInputManager(globalContext.Load().(*common.Context))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %q %q", html.EscapeString(r.URL.Path), r.TLS.PeerCertificates[0].Subject.CommonName)
	})
	gradeRe := regexp.MustCompile("/run/grade/(\\d+)/?")
	http.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
		ctx := globalContext.Load().(*common.Context)
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
		run, err := common.NewRun(id, ctx)
		if err != nil {
			ctx.Log.Error(err.Error(), "id", id)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		runCtx, err := grader.NewRunContext(run, ctx)
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
		ctx := globalContext.Load().(*common.Context)
		ctx.Log.Debug("requesting run",
			"proto", r.Proto, "client", r.TLS.PeerCertificates[0].Subject.CommonName)
		select {
		case <-w.(http.CloseNotifier).CloseNotify():
			ctx.Log.Debug("client gone",
				"client", r.TLS.PeerCertificates[0].Subject.CommonName)
		case runCtx := <-runs:
			ctx.Log.Debug("served run", "run", runCtx.Run,
				"client", r.TLS.PeerCertificates[0].Subject.CommonName)
			w.Header().Set("Content-Type", "text/json; charset=utf-8")
			encoder := json.NewEncoder(w)
			encoder.Encode(runCtx.Run)
			runCtx.Input.Release()
		}
	})
	inputRe := regexp.MustCompile("/input/([a-f0-9]{40})/?")
	http.HandleFunc("/input/", func(w http.ResponseWriter, r *http.Request) {
		ctx := globalContext.Load().(*common.Context)
		res := inputRe.FindStringSubmatch(r.URL.Path)
		if res == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hash := res[1]
		input, err := common.DefaultInputManager.Get(hash)
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

	if err := startServer(globalContext.Load().(*common.Context)); err != nil {
		panic(err)
	}
}
