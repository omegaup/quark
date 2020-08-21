package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"golang.org/x/net/http2"
)

var (
	// One-shot mode: Performs a single operation and exits.
	oneshot = flag.String("oneshot", "",
		"Perform one action and return. Valid values are 'benchmark' and 'run'.")
	verbose = flag.Bool("verbose", false, "Enable verbose logging in oneshot mode.")
	request = flag.String("request", "",
		"With -oneshot=run, the path to the JSON request.")
	source = flag.String("source", "",
		"With -oneshot=run, the path to the source file.")
	input = flag.String("input", "",
		"With -oneshot=run, the path to the input directory, which should be a checkout of a problem.")
	resultsOutputDirectory = flag.String("results", "",
		"With -oneshot=run, the path to the directory to copy the results to.")
	debug = flag.Bool("debug", false, "Enables debug in oneshot mode.")

	version    = flag.Bool("version", false, "Print the version and exit")
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	noop       = flag.Bool("noop-sandbox", false, "Use the no-op sandbox (always returns AC)")
	configPath = flag.String("config", "/etc/omegaup/runner/config.json",
		"Runner configuration file")
	globalContext atomic.Value
	ioLock        sync.Mutex
	inputManager  *common.InputManager
	sandbox       runner.Sandbox

	// ProgramVersion is the version of the code from which the binary was built from.
	ProgramVersion string
)

func isOneShotMode() bool {
	return *oneshot == "benchmark" || *oneshot == "run"
}

func loadContext() error {
	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer f.Close()
	config, err := common.NewConfig(f)
	if isOneShotMode() {
		config.Logging.File = "stderr"
		config.Tracing.Enabled = false
		if *verbose {
			config.Logging.Level = "debug"
		}
	}

	ctx, err := common.NewContext(config, "runner")
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func runOneshotBenchmark(ctx *common.Context, sandbox runner.Sandbox) {
	results, err := runner.RunHostBenchmark(
		ctx,
		inputManager,
		sandbox,
		&ioLock,
	)
	if err != nil {
		ctx.Log.Error("Failed to run benchmark", "err", err)
		return
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		ctx.Log.Error("Failed to encode JSON", "err", err)
	}
}

func runOneshotRun(ctx *common.Context, sandbox runner.Sandbox) {
	if *input == "" {
		ctx.Log.Error("Missing -input parameter")
		return
	}
	var run common.Run
	if *request != "" {
		f, err := os.Open(*request)
		if err != nil {
			ctx.Log.Error("Error opening request", "err", err)
			return
		}
		defer f.Close()

		if err := json.NewDecoder(f).Decode(&run); err != nil {
			ctx.Log.Error("Error reading request", "err", err)
			return
		}
	} else if *source != "" {
		b, err := ioutil.ReadFile(*source)
		if err != nil {
			ctx.Log.Error("Error opening source", "err", err)
			return
		}
		run.Source = string(b)
		extension := path.Ext(*source)
		if extension == "" {
			ctx.Log.Error("Source path does not contain the language as extension", "source", *source)
			return
		}
		run.Language = extension[1:]
		run.MaxScore = base.FloatToRational(100.0)
	} else {
		ctx.Log.Error("Missing -request or -source parameters")
		return
	}

	if *debug {
		run.Debug = true
	}
	run.InputHash = oneshotInputHash

	inputRef, err := inputManager.Add(
		run.InputHash,
		newOneshotInputFactory(*input),
	)
	if err != nil {
		ctx.Log.Error("Error loading input", "hash", run.InputHash, "err", err)
		return
	}
	defer inputRef.Release()

	runRoot := path.Join(
		ctx.Config.Runner.RuntimePath,
		"grade",
		strconv.FormatUint(run.AttemptID, 10),
	)
	if *resultsOutputDirectory != "" && !ctx.Config.Runner.PreserveFiles {
		ctx.Config.Runner.PreserveFiles = true
		defer os.RemoveAll(runRoot)
	}

	results, err := runner.Grade(ctx, nil, &run, inputRef.Input, sandbox)
	if err != nil {
		ctx.Log.Error("Error grading run", "err", err)
		return
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		ctx.Log.Error("Failed to encode JSON", "err", err)
	}

	if *resultsOutputDirectory != "" {
		if err := filepath.Walk(
			runRoot,
			func(srcPath string, info os.FileInfo, err error) error {
				if err != nil {
					ctx.Log.Error("Failed to walk", "dir", runRoot, "path", srcPath, "err", err)
					return err
				}
				if !info.Mode().IsRegular() {
					return nil
				}

				relDstPath, err := filepath.Rel(runRoot, srcPath)
				if err != nil {
					ctx.Log.Error("Failed to relativize path", "dir", runRoot, "path", srcPath, "err", err)
					return err
				}
				dstPath := path.Join(*resultsOutputDirectory, relDstPath)
				if err := os.MkdirAll(path.Dir(dstPath), 0755); err != nil {
					ctx.Log.Error(
						"Failed to create intermediate directory",
						"dir", runRoot,
						"path", srcPath,
						"destination", dstPath,
						"err", err,
					)
					return err
				}

				srcFile, err := os.Open(srcPath)
				if err != nil {
					ctx.Log.Error("Failed to open source file", "path", srcPath, "err", err)
					return err
				}
				defer srcFile.Close()

				dstFile, err := os.Create(dstPath)
				if err != nil {
					ctx.Log.Error("Failed to create target file", "path", dstPath, "err", err)
					return err
				}
				defer dstFile.Close()

				if _, err := io.Copy(dstFile, srcFile); err != nil {
					ctx.Log.Error("Failed to copy file", "path", dstPath, "err", err)
					return err
				}
				return nil
			},
		); err != nil {
			ctx.Log.Error("Failed to encode JSON", "err", err)
		}
	}
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	flag.Parse()

	if *version {
		fmt.Printf("omegaup-runner %s\n", ProgramVersion)
		return
	}

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := globalContext.Load().(*common.Context)
	if *noop {
		sandbox = &runner.NoopSandbox{}
	} else {
		omegajailRoot, err := filepath.Abs(ctx.Config.Runner.OmegajailRoot)
		if err != nil {
			ctx.Log.Error("Failed to get omegajail root", "err", err)
			os.Exit(1)
		}
		sandbox = runner.NewOmegajailSandbox(omegajailRoot)
	}

	if isOneShotMode() {
		tmpdir, err := ioutil.TempDir("", "quark-runner-oneshot")
		if err != nil {
			ctx.Log.Error("Failed to create temporary directory", "err", err)
			os.Exit(1)
		}
		if os.Getenv("PRESERVE") != "" {
			ctx.Config.Runner.PreserveFiles = true
		}
		if !ctx.Config.Runner.PreserveFiles {
			defer os.RemoveAll(tmpdir)
		}
		ctx.Config.Runner.RuntimePath = tmpdir
	}

	expvar.Publish("config", &globalContext.Load().(*common.Context).Config)
	inputManager = common.NewInputManager(ctx)
	inputPath := path.Join(ctx.Config.Runner.RuntimePath, "input")

	if isOneShotMode() {
		if *oneshot == "benchmark" {
			runOneshotBenchmark(ctx, sandbox)
		} else if *oneshot == "run" {
			runOneshotRun(ctx, sandbox)
		} else {
			ctx.Log.Error("Unknown oneshot mode", "mode", *oneshot)
		}
		return
	}

	go inputManager.PreloadInputs(
		inputPath,
		runner.NewCachedInputFactory(inputPath),
		&ioLock,
	)
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if !*insecure {
		cert, err := ioutil.ReadFile(ctx.Config.TLS.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)
		keyPair, err := tls.LoadX509KeyPair(
			ctx.Config.TLS.CertFile,
			ctx.Config.TLS.KeyFile,
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

	baseURL, err := url.Parse(ctx.Config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)
	cancelContext, cancel := context.WithCancel(ctx.Context)
	ctx.Context = cancelContext

	setupMetrics(ctx)
	var wg sync.WaitGroup
	if !*noop {
		// Only run the benchmark loop if the sandbox is actually running.
		// Otherwise the results are moot.
		go benchmarkLoop(ctx, &wg)
	}
	go runnerLoop(ctx, &wg, client, baseURL)

	ctx.Log.Info(
		"omegaUp runner ready",
		"version", ProgramVersion,
	)
	daemon.SdNotify(false, "READY=1")

	<-stopChan

	daemon.SdNotify(false, "STOPPING=1")
	ctx.Log.Info("Shutting down server...")

	cancel()
	wg.Wait()

	ctx.Close()
	ctx.Log.Info("Server gracefully stopped.")
}
