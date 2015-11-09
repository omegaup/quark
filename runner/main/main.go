package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
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

	baseURL, err := url.Parse(ctx.Config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}

	ctx.Log.Info("omegaUp runner ready to serve")

	var sleepTime int64 = 1

	for {
		if err := processRun(ctx, client, baseURL); err != nil {
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

func processRun(ctx *common.Context, client *http.Client, baseURL *url.URL) error {
	requestURL, err := baseURL.Parse("run/request/")
	if err != nil {
		panic(err)
	}
	resp, err := client.Get(requestURL.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var run common.Run
	if err := decoder.Decode(&run); err != nil {
		return err
	}

	uploadURL, err := baseURL.Parse(fmt.Sprintf("run/%d/results/", run.ID))
	if err != nil {
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
	defer input.Release()
	result, err := runner.Grade(ctx, client, baseURL, &run, input)
	if err != nil {
		ctx.Log.Error("Error while grading", "err", err)
	}
	var resultBytes bytes.Buffer
	encoder := json.NewEncoder(&resultBytes)
	if err := encoder.Encode(result); err != nil {
		return err
	}
	response, err := client.Post(uploadURL.String(), "text/json", &resultBytes)
	if err != nil {
		return err
	}
	response.Body.Close()
	return nil
}
