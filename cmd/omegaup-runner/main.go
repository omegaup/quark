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
	"math/big"
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	nrtracing "github.com/omegaup/go-base/tracing/newrelic/v3"
	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"github.com/omegaup/quark/runner/ci"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/newrelic/go-agent/v3/newrelic"
	errors "github.com/pkg/errors"
	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2"
)

var (
	// One-shot mode: Performs a single operation and exits.
	oneshot = flag.String("oneshot", "",
		"Perform one action and return. Valid values are 'benchmark', 'run', and 'ci'.")
	verbose = flag.Bool("verbose", false, "Enable verbose logging in oneshot mode.")
	request = flag.String("request", "",
		"With -oneshot=run, the path to the JSON request.")
	source = flag.String("source", "",
		"With -oneshot=run, the path to the source file.")
	input = flag.String("input", "",
		"With -oneshot={run,ci}, the path to the input directory, which should be a checkout of a problem.")
	resultsOutputDirectory = flag.String("results", "",
		"With -oneshot={run,ci}, the path to the directory to copy the results to.")
	outputsDirectory = flag.String("outputs", "",
		"With -oneshot=ci and an output generator, the path to the directory to copy the .out files to.")
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
	return *oneshot == "benchmark" || *oneshot == "run" || *oneshot == "ci"
}

func loadContext() error {
	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer f.Close()
	config, err := common.NewConfig(f)
	if isOneShotMode() {
		if *verbose {
			config.Logging.Level = "debug"
		}
	}

	ctx, err := common.NewContext(config)
	if err != nil {
		return err
	}

	res, err := http.Get("https://ifconfig.me/ip")
	if err != nil {
		ctx.Log.Error(
			"Failed to get public IP",
			map[string]any{
				"err": err,
			},
		)
	} else {
		ipBytes, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			ctx.Log.Error(
				"Failed to read public IP",
				map[string]any{
					"err": err,
				},
			)
		} else {
			ip := strings.TrimSpace(string(ipBytes))

			if !httpguts.ValidHeaderFieldValue(ip) {
				ctx.Log.Error(
					"Public IP is invalid",
					map[string]any{
						"ip": ip,
					},
				)
			} else {
				ctx.Config.Runner.PublicIP = ip
			}
		}
	}

	globalContext.Store(ctx)
	return nil
}

func persistResults(ctx *common.Context, root string, target string) error {
	return filepath.Walk(
		root,
		func(srcPath string, info os.FileInfo, err error) error {
			if err != nil {
				ctx.Log.Error(
					"Failed to walk",
					map[string]any{
						"dir":  root,
						"path": srcPath,
						"err":  err,
					},
				)
				return err
			}
			if !info.Mode().IsRegular() {
				// Skip anything that is not a regular file.
				return nil
			}

			relDstPath, err := filepath.Rel(root, srcPath)
			if err != nil {
				ctx.Log.Error(
					"Failed to relativize path",
					map[string]any{
						"dir":  root,
						"path": srcPath,
						"err":  err,
					},
				)
				return err
			}
			dstPath := path.Join(target, relDstPath)
			if err := os.MkdirAll(path.Dir(dstPath), 0755); err != nil {
				ctx.Log.Error(
					"Failed to create intermediate directory",
					map[string]any{
						"dir":         root,
						"path":        srcPath,
						"destination": dstPath,
						"err":         err,
					},
				)
				return err
			}

			srcFile, err := os.Open(srcPath)
			if err != nil {
				ctx.Log.Error(
					"Failed to open source file",
					map[string]any{
						"path": srcPath,
						"err":  err,
					},
				)
				return err
			}
			defer srcFile.Close()

			dstFile, err := os.Create(dstPath)
			if err != nil {
				ctx.Log.Error(
					"Failed to create target file",
					map[string]any{
						"path": dstPath,
						"err":  err,
					},
				)
				return err
			}
			defer dstFile.Close()

			if _, err := io.Copy(dstFile, srcFile); err != nil {
				ctx.Log.Error(
					"Failed to copy file",
					map[string]any{
						"path": dstPath,
						"err":  err,
					},
				)
				return err
			}
			return nil
		},
	)
}

func runOneshotBenchmark(ctx *common.Context, sandbox runner.Sandbox) {
	results, err := runner.RunHostBenchmark(
		ctx,
		inputManager,
		sandbox,
		&ioLock,
	)
	if err != nil {
		ctx.Log.Error(
			"Failed to run benchmark",
			map[string]any{
				"err": err,
			},
		)
		return
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		ctx.Log.Error(
			"Failed to encode JSON",
			map[string]any{
				"err": err,
			},
		)
	}
}

func runOneshotRun(ctx *common.Context, sandbox runner.Sandbox) {
	if *input == "" {
		ctx.Log.Error("Missing -input parameter", nil)
		return
	}
	var run common.Run
	if *request != "" {
		f, err := os.Open(*request)
		if err != nil {
			ctx.Log.Error(
				"Error opening request",
				map[string]any{
					"err": err,
				},
			)
			return
		}
		defer f.Close()

		if err := json.NewDecoder(f).Decode(&run); err != nil {
			ctx.Log.Error(
				"Error reading request",
				map[string]any{
					"err": err,
				},
			)
			return
		}
	} else if *source != "" {
		b, err := ioutil.ReadFile(*source)
		if err != nil {
			ctx.Log.Error(
				"Error opening source",
				map[string]any{
					"err": err,
				},
			)
			return
		}
		run.Source = string(b)
		extension := path.Ext(*source)
		if extension == "" {
			ctx.Log.Error(
				"Source path does not contain the language as extension",
				map[string]any{
					"source": *source,
				},
			)
			return
		}
		run.Language = extension[1:]
		run.MaxScore = base.FloatToRational(100.0)
	} else {
		ctx.Log.Error("Missing -request or -source parameters", nil)
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
		ctx.Log.Error(
			"Error loading input",
			map[string]any{
				"hash": run.InputHash,
				"err":  err,
			},
		)
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
		ctx.Log.Error(
			"Error grading run",
			map[string]any{
				"err": err,
			},
		)
		return
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		ctx.Log.Error(
			"Failed to encode JSON",
			map[string]any{
				"err": err,
			},
		)
	}
	if *resultsOutputDirectory != "" {
		if err := persistResults(ctx, runRoot, *resultsOutputDirectory); err != nil {
			ctx.Log.Error(
				"Failed to persist results",
				map[string]any{
					"err": err,
				},
			)
		}
	}
}

func runOneshotCI(ctx *common.Context, sandbox runner.Sandbox) *ci.Report {
	report := &ci.Report{
		Problem:   path.Base(*input),
		StartTime: time.Now(),
		State:     ci.StateError,
	}
	defer func() {
		report.UpdateState()
		finishTime := time.Now()
		report.FinishTime = &finishTime
		duration := base.Duration(report.FinishTime.Sub(report.StartTime))
		report.Duration = &duration
	}()
	if *input == "" {
		ctx.Log.Error("Missing -input parameter", nil)
		report.State = ci.StateSkipped
		report.ReportError = &ci.ReportError{Error: errors.New("Missing -input parameter")}
		return report
	}
	if *outputsDirectory != "" {
		if err := os.MkdirAll(*outputsDirectory, 0755); err != nil {
			ctx.Log.Error(
				"Failed to create outputs directory",
				map[string]any{
					"dir": *outputsDirectory,
					"err": err,
				},
			)
			report.State = ci.StateSkipped
			report.ReportError = &ci.ReportError{
				Error: errors.Wrapf(
					err,
					"failed to create outputs directory",
				),
			}
			return report
		}
	}

	problemFiles, err := common.NewProblemFilesFromFilesystem(*input)
	if err != nil {
		ctx.Log.Error(
			"Error loading input",
			map[string]any{
				"path": *input,
				"err":  err,
			},
		)
		report.State = ci.StateSkipped
		report.ReportError = &ci.ReportError{Error: err}
		return report
	}
	runConfig, err := ci.NewRunConfig(problemFiles, *outputsDirectory != "")
	if err != nil {
		ctx.Log.Error(
			"Error loading run configuration",
			map[string]any{
				"path": *input,
				"err":  err,
			},
		)
		report.State = ci.StateSkipped
		report.ReportError = &ci.ReportError{Error: err}
		return report
	}

	report.State = ci.StatePassed

	if runConfig.OutGeneratorConfig != nil {
		err = (func() error {
			factory, err := common.NewLiteralInputFactory(
				runConfig.OutGeneratorConfig.Input,
				ctx.Config.Grader.RuntimePath,
				common.LiteralPersistRunner,
			)
			if err != nil {
				ctx.Log.Error(
					"Error loading input",
					map[string]any{
						"config": runConfig.OutGeneratorConfig,
						"err":    err,
					},
				)
				return err
			}

			run := common.Run{
				InputHash:   factory.Hash(),
				AttemptID:   uint64(len(runConfig.TestConfigs)),
				MaxScore:    big.NewRat(1, 1),
				Source:      runConfig.OutGeneratorConfig.Solution.Source,
				Language:    runConfig.OutGeneratorConfig.Solution.Language,
				ProblemName: report.Problem,
			}
			if *debug {
				run.Debug = true
			}

			inputRef, err := inputManager.Add(
				run.InputHash,
				factory,
			)
			if err != nil {
				ctx.Log.Error(
					"Error loading input",
					map[string]any{
						"config": runConfig.OutGeneratorConfig,
						"err":    err,
					},
				)
				return err
			}
			defer inputRef.Release()

			runRoot := path.Join(
				ctx.Config.Runner.RuntimePath,
				"grade",
				strconv.FormatUint(run.AttemptID, 10),
			)
			if !ctx.Config.Runner.PreserveFiles {
				defer os.RemoveAll(runRoot)
			}

			result, err := runner.Grade(ctx, nil, &run, inputRef.Input, sandbox)
			if err != nil {
				ctx.Log.Error(
					"Error generating outputs",
					map[string]any{
						"config": runConfig.OutGeneratorConfig,
						"err":    err,
					},
				)
				return err
			}
			if result.Verdict != "AC" && result.Verdict != "PA" && result.Verdict != "WA" {
				ctx.Log.Error(
					"Error generating outputs",
					map[string]any{
						"config":       runConfig.OutGeneratorConfig,
						"compileError": result.CompileError,
						"err":          err,
					},
				)
				if result.CompileError != nil {
					return errors.Errorf(
						"expecting a verdict of {AC, PA, WA}; got %s:\n%q",
						result.Verdict,
						*result.CompileError,
					)
				}
				return errors.Errorf(
					"expecting a verdict of {AC, PA, WA}; got %s",
					result.Verdict,
				)
			}

			for pathCaseName := range runConfig.OutGeneratorConfig.Input.Cases {
				if strings.HasPrefix(pathCaseName, "cases/") {
					srcPath := path.Join(runRoot, fmt.Sprintf("%s.out", pathCaseName))
					outContents, err := ioutil.ReadFile(srcPath)
					if err != nil {
						ctx.Log.Error(
							"Failed to open source file",
							map[string]any{
								"path": srcPath,
								"err":  err,
							},
						)
						return err
					}

					caseName := strings.TrimPrefix(pathCaseName, "cases/")
					runConfig.Input.Cases[caseName].ExpectedOutput = string(outContents)
				}

				if *outputsDirectory != "" {
					srcPath := path.Join(runRoot, fmt.Sprintf("%s.out", pathCaseName))
					srcFile, err := os.Open(srcPath)
					if err != nil {
						ctx.Log.Error(
							"Failed to open source file",
							map[string]any{
								"path": srcPath,
								"err":  err,
							},
						)
						return err
					}
					dstPath := path.Join(*outputsDirectory, fmt.Sprintf("%s.out", pathCaseName))
					if err := os.MkdirAll(path.Dir(dstPath), 0o755); err != nil {
						srcFile.Close()
						ctx.Log.Error(
							"Failed to create target file directory",
							map[string]any{
								"path": dstPath,
								"err":  err,
							},
						)
						return err
					}
					dstFile, err := os.Create(dstPath)
					if err != nil {
						srcFile.Close()
						ctx.Log.Error(
							"Failed to create target file",
							map[string]any{
								"path": dstPath,
								"err":  err,
							},
						)
						return err
					}
					_, err = io.Copy(dstFile, srcFile)
					srcFile.Close()
					dstFile.Close()
					if err != nil {
						ctx.Log.Error(
							"Failed to copy file",
							map[string]any{
								"path": dstPath,
								"err":  err,
							},
						)
						return err
					}
				}
			}
			return nil
		})()
		if err != nil {
			ctx.Log.Error(
				"Error generating .out files",
				map[string]any{
					"path": *input,
					"err":  err,
				},
			)
			report.State = ci.StateSkipped
			report.ReportError = &ci.ReportError{
				Error: errors.Wrapf(
					err,
					"failed to generate .out files for %s",
					problemFiles.String(),
				),
			}
			return report
		}
	}

	for i, testConfig := range runConfig.TestConfigs {
		(func() {
			testConfig.Test.StartTime = time.Now()
			testConfig.Test.State = ci.StateError
			defer func() {
				finishTime := time.Now()
				testConfig.Test.FinishTime = &finishTime
				duration := base.Duration(testConfig.Test.FinishTime.Sub(testConfig.Test.StartTime))
				testConfig.Test.Duration = &duration
			}()

			factory, err := common.NewLiteralInputFactory(
				testConfig.Input,
				ctx.Config.Grader.RuntimePath,
				common.LiteralPersistRunner,
			)
			if err != nil {
				ctx.Log.Error(
					"Error loading input",
					map[string]any{
						"test": testConfig,
						"err":  err,
					},
				)
				testConfig.Test.State = ci.StateError
				testConfig.Test.ReportError = &ci.ReportError{Error: err}
				return
			}

			run := common.Run{
				InputHash:   factory.Hash(),
				AttemptID:   uint64(i),
				MaxScore:    big.NewRat(1, 1),
				Source:      testConfig.Solution.Source,
				Language:    testConfig.Solution.Language,
				ProblemName: report.Problem,
			}
			if *debug {
				run.Debug = true
			}

			inputRef, err := inputManager.Add(
				run.InputHash,
				factory,
			)
			if err != nil {
				ctx.Log.Error(
					"Error loading input",
					map[string]any{
						"test": testConfig,
						"err":  err,
					},
				)
				testConfig.Test.State = ci.StateError
				testConfig.Test.ReportError = &ci.ReportError{Error: err}
				return
			}
			defer inputRef.Release()

			runRoot := path.Join(
				ctx.Config.Runner.RuntimePath,
				"grade",
				strconv.FormatUint(run.AttemptID, 10),
			)
			if *resultsOutputDirectory != "" {
				defer func() {
					if err := persistResults(
						ctx,
						runRoot,
						path.Join(
							*resultsOutputDirectory,
							strconv.FormatUint(run.AttemptID, 10),
						),
					); err != nil {
						ctx.Log.Error(
							"Failed to persist results",
							map[string]any{
								"err": err,
							},
						)
					}
					if !ctx.Config.Runner.PreserveFiles {
						os.RemoveAll(runRoot)
					}
				}()
			}

			result, err := runner.Grade(ctx, nil, &run, inputRef.Input, sandbox)
			if err != nil {
				ctx.Log.Error(
					"Error grading run",
					map[string]any{
						"test": testConfig,
						"err":  err,
					},
				)
				testConfig.Test.State = ci.StateError
				testConfig.Test.ReportError = &ci.ReportError{Error: err}
				return
			}
			testConfig.Test.SetResult(result)
		})()
		report.Tests = append(report.Tests, testConfig.Test)
	}
	return report
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
			ctx.Log.Error(
				"Failed to get omegajail root",
				map[string]any{
					"err": err,
				},
			)
			os.Exit(1)
		}
		oj := runner.NewOmegajailSandbox(omegajailRoot)
		if *oneshot == "ci" {
			// Allow sigsys to use the fallback detector when running in CI.
			oj.AllowSigsysFallback = true
			// Disable sandboxing when running inside Docker.
			oj.DisableSandboxing = true
		}
		sandbox = oj
	}

	if isOneShotMode() {
		tmpdir, err := ioutil.TempDir("", "quark-runner-oneshot")
		if err != nil {
			ctx.Log.Error(
				"Failed to create temporary directory",
				map[string]any{
					"err": err,
				},
			)
			os.Exit(1)
		}
		if os.Getenv("PRESERVE") != "" || *oneshot == "ci" {
			ctx.Config.Runner.PreserveFiles = true
		}
		if !ctx.Config.Runner.PreserveFiles {
			defer os.RemoveAll(tmpdir)
		}
		ctx.Config.Runner.RuntimePath = tmpdir
	}

	var app *newrelic.Application
	if ctx.Config.NewRelic.License != "" {
		var err error
		app, err = newrelic.NewApplication(
			newrelic.ConfigAppName(ctx.Config.NewRelic.AppName),
			newrelic.ConfigLicense(ctx.Config.NewRelic.License),
			newrelic.ConfigLogger(ctx.Log),
			newrelic.ConfigDistributedTracerEnabled(true),
		)
		if err != nil {
			panic(err)
		}
	}
	ctx.Tracing = nrtracing.New(app)

	expvar.Publish("config", &globalContext.Load().(*common.Context).Config)
	inputManager = common.NewInputManager(ctx)
	inputPath := path.Join(ctx.Config.Runner.RuntimePath, "input")

	if isOneShotMode() {
		if *oneshot == "benchmark" {
			runOneshotBenchmark(ctx, sandbox)
		} else if *oneshot == "run" {
			runOneshotRun(ctx, sandbox)
		} else if *oneshot == "ci" {
			if *resultsOutputDirectory != "" {
				ctx.Config.Runner.PreserveFiles = true
				ctx = ctx.DebugContext(nil)
			}
			if *outputsDirectory != "" {
				ctx.Config.Runner.PreserveFiles = true
			}
			report := runOneshotCI(ctx, sandbox)
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(report); err != nil {
				ctx.Log.Error(
					"Failed to encode JSON",
					map[string]any{
						"err": err,
					},
				)
			}
			if *resultsOutputDirectory != "" {
				{
					err := os.WriteFile(path.Join(*resultsOutputDirectory, "ci.log"), ctx.LogBuffer(), 0o644)
					if err != nil {
						ctx.Log.Error(
							"Failed to create log file",
							map[string]any{
								"err": err,
							},
						)
					}
				}
				{
					f, err := os.Create(path.Join(*resultsOutputDirectory, "report.json"))
					if err != nil {
						ctx.Log.Error(
							"Failed to create report file",
							map[string]any{
								"err": err,
							},
						)
					} else {
						encoder := json.NewEncoder(f)
						encoder.SetIndent("", "  ")
						err = encoder.Encode(report)
						f.Close()
						if err != nil {
							ctx.Log.Error(
								"Failed to write report file",
								map[string]any{
									"err": err,
								},
							)
						}
					}
				}
			}
		} else {
			ctx.Log.Error(
				"Unknown oneshot mode",
				map[string]any{
					"mode": *oneshot,
				},
			)
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

	stopChan := make(chan os.Signal, 1)
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
		map[string]any{
			"version": ProgramVersion,
		},
	)
	daemon.SdNotify(false, "READY=1")

	<-stopChan

	daemon.SdNotify(false, "STOPPING=1")
	ctx.Log.Info("Shutting down server...", nil)

	cancel()
	wg.Wait()

	ctx.Close()
	ctx.Log.Info("Server gracefully stopped.", nil)
}
