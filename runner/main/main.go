package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

var (
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	configPath = flag.String("config", "/etc/omegaup/grader/config.json",
		"Grader configuration file")
	globalContext atomic.Value
	ioLock        sync.Mutex
)

func loadContext() error {
	ctx, err := common.NewContext(*configPath)
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

	ctx := globalContext.Load().(*common.Context)
	expvar.Publish("config", &globalContext.Load().(*common.Context).Config)
	common.InitInputManager(ctx)
	go runner.PreloadInputs(ctx, &ioLock)
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

	baseUrl, err := url.Parse(ctx.Config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}
	requestUrl, err := baseUrl.Parse("run/request")
	if err != nil {
		panic(err)
	}

	ctx.Log.Info("omegaUp runner ready to serve")

	var sleepTime int64 = 1

	for {
		if err := processRun(ctx, client, requestUrl.String()); err != nil {
			ctx.Log.Error("error grading run", "err", err)
			time.Sleep(time.Duration(sleepTime) * time.Second)
			if sleepTime < 64 {
				sleepTime *= 2
			}
		} else {
			sleepTime = 1
		}
	}
}

func processRun(ctx *common.Context, client *http.Client, requestURL string) error {
	resp, err := client.Get(requestURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var run common.Run
	if err := decoder.Decode(&run); err != nil {
		return err
	}

	// Make sure no other I/O is being made while we grade this run.
	ioLock.Lock()
	defer ioLock.Unlock()

	input, err := common.DefaultInputManager.Add(run.InputHash,
		runner.NewRunnerInputFactory(&run, client, &ctx.Config))
	if err != nil {
		return err
	}
	runner.Grade(&run, input)
	input.Release()
	return nil
}
