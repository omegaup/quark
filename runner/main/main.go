package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"github.com/omegaup/quark/context"
	"github.com/omegaup/quark/queue"
	"github.com/omegaup/quark/runner"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync/atomic"
)

var (
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	configPath = flag.String("config", "/etc/omegaup/grader/config.json",
		"Grader configuration file")
	globalContext atomic.Value
)

func loadContext() error {
	ctx, err := context.NewContext(*configPath)
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func main() {
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := globalContext.Load().(*context.Context)
	expvar.Publish("config", &globalContext.Load().(*context.Context).Config)
	context.InitInputManager(ctx)
	var client *http.Client
	if *insecure {
		client = http.DefaultClient
	} else {
		cert, err := ioutil.ReadFile(ctx.Config.TLS.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)
		keyPair, err := tls.LoadX509KeyPair(ctx.Config.TLS.CertFile, ctx.Config.TLS.KeyFile)
		if err != nil {
			panic(err)
		}
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				Certificates: []tls.Certificate{keyPair},
				RootCAs:      certPool,
				ClientAuth:   tls.RequireAndVerifyClientCert,
			},
			DisableCompression: true,
		}
		client = &http.Client{Transport: tr}
	}

	for {
		baseUrl, err := url.Parse(ctx.Config.Runner.GraderURL)
		if err != nil {
			panic(err)
		}
		requestUrl, err := baseUrl.Parse("run/request")
		if err != nil {
			panic(err)
		}
		resp, err := client.Get(requestUrl.String())
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		decoder := json.NewDecoder(resp.Body)
		var run queue.Run
		if err := decoder.Decode(&run); err != nil {
			panic(err)
		}
		input, err := context.DefaultInputManager.Add(run.InputHash,
			runner.NewRunnerInputFactory(&run, client, &ctx.Config))
		if err != nil {
			panic(err)
		}
		ctx.Log.Info("Grading", "run", run, "input", input)
		input.Release()
	}
}
