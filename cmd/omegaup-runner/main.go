package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"github.com/pkg/errors"
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
				if info.IsDir() {
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

	setupMetrics(ctx)
	ctx.Log.Info(
		"omegaUp runner ready",
		"version", ProgramVersion,
	)
	daemon.SdNotify(false, "READY=1")

	if !*noop {
		// Only run the benchmark loop if the sandbox is actually running.
		// Otherwise the results are moot.
		go func() {
			for {
				results, err := runner.RunHostBenchmark(
					ctx,
					inputManager,
					sandbox,
					&ioLock,
				)
				if err != nil {
					ctx.Log.Error("Failed to run benchmark", "err", err)
				} else {
					ctx.Log.Info("Benchmark successful", "results", results)
				}
				gaugesUpdate(results)
				time.Sleep(time.Duration(1) * time.Minute)
			}
		}()
	}

	var sleepTime float32 = 1

	for {
		if err := processRun(ctx, client, baseURL); err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// Timeouts are expected. Just retry.
				sleepTime = 1
				continue
			}
			ctx.Log.Error("error grading run", "err", err)
			// Randomized exponential backoff.
			time.Sleep(time.Duration(rand.Float32()*sleepTime) * time.Second)
			if sleepTime < 64 {
				sleepTime *= 2
			}
		} else {
			sleepTime = 1
		}
	}
}

// channelBuffer is a buffer that implements io.Reader, io.Writer, and
// io.WriterTo. Write() stores the incoming slices in a []byte channel, which
// are then consumed when either Read() or WriteTo() are called.
type channelBuffer struct {
	chunks       chan []byte
	currentChunk []byte
}

func newChannelBuffer() *channelBuffer {
	return &channelBuffer{
		chunks: make(chan []byte, 0),
	}
}

func (cb *channelBuffer) closeChannel() {
	close(cb.chunks)
}

func (cb *channelBuffer) Write(buf []byte) (int, error) {
	// Copy the buffer since we cannot guarantee that the caller will not write
	// to it again while we are waiting for the other end to read from it.
	innerbuf := make([]byte, len(buf))
	copy(innerbuf, buf)

	cb.chunks <- innerbuf
	return len(innerbuf), nil
}

func (cb *channelBuffer) WriteTo(w io.Writer) (int64, error) {
	totalWritten := int64(0)
	for {
		if cb.currentChunk == nil {
			c, ok := <-cb.chunks
			if !ok {
				return totalWritten, nil
			}
			cb.currentChunk = c
		}

		written, err := w.Write(cb.currentChunk)
		totalWritten += int64(written)
		if err != nil {
			return totalWritten, err
		}
		if written == len(cb.currentChunk) {
			cb.currentChunk = nil
		} else if written > 0 {
			cb.currentChunk = cb.currentChunk[written:]
		}
	}
}

func (cb *channelBuffer) Close() error {
	// If there are any chunks remaining, they should be drained before
	// returning.  The other goroutine should eventually close the channel.
	for range cb.chunks {
	}
	return nil
}

func (cb *channelBuffer) Read(buf []byte) (int, error) {
	if cb.currentChunk == nil {
		c, ok := <-cb.chunks
		if !ok {
			return 0, io.EOF
		}
		cb.currentChunk = c
	}

	written := copy(buf, cb.currentChunk)
	if written == len(cb.currentChunk) {
		cb.currentChunk = nil
	} else {
		cb.currentChunk = cb.currentChunk[written:]
	}

	return written, nil
}

func processRun(
	parentCtx *common.Context,
	client *http.Client,
	baseURL *url.URL,
) error {
	requestURL, err := baseURL.Parse("run/request/")
	if err != nil {
		panic(err)
	}
	resp, err := client.Get(requestURL.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return errors.Errorf("non-2xx error code returned: %d", resp.StatusCode)
	}

	ctx := parentCtx.DebugContext()
	syncID, err := strconv.ParseUint(resp.Header.Get("Sync-ID"), 10, 64)
	if err != nil {
		return errors.Wrap(err, "failed to parse the Sync-ID header")
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewReceiverClockSyncEvent(syncID))

	decoder := json.NewDecoder(resp.Body)
	var run common.Run
	if err := decoder.Decode(&run); err != nil {
		return errors.Wrap(err, "failed to parse the run request body")
	}
	uploadURL, err := baseURL.Parse(fmt.Sprintf("run/%d/results/", run.AttemptID))
	if err != nil {
		return errors.Wrap(err, "failed to create the result upload URL")
	}

	finished := make(chan error, 1)

	if err = gradeAndUploadResults(
		ctx,
		client,
		uploadURL.String(),
		&run,
		finished,
	); err != nil {
		return err
	}

	return <-finished
}

func gradeAndUploadResults(
	ctx *common.Context,
	client *http.Client,
	uploadURL string,
	run *common.Run,
	finished chan<- error,
) error {
	requestBody := newChannelBuffer()
	defer requestBody.closeChannel()
	multipartWriter := multipart.NewWriter(requestBody)
	defer multipartWriter.Close()
	go func() {
		defer requestBody.Close()
		req, err := http.NewRequest("POST", uploadURL, requestBody)
		if err != nil {
			finished <- err
			close(finished)
			return
		}
		req.Header.Add("Content-Type", multipartWriter.FormDataContentType())
		response, err := client.Do(req)
		if err != nil {
			finished <- err
			close(finished)
			return
		}
		response.Body.Close()
		finished <- nil
		close(finished)
	}()

	result, err := gradeRun(ctx, client, run, multipartWriter)
	if err != nil {
		// Still try to send the details
		ctx.Log.Error("Error grading run", "err", err)
		result = runner.NewRunResult("JE", run.MaxScore)
	}

	if *noop {
		runner.NoopSandboxFixupResult(result)
	}

	// Send results.
	resultWriter, err := multipartWriter.CreateFormFile("file", "details.json")
	if err != nil {
		ctx.Log.Error("Error sending details.json", "err", err)
		return err
	}
	encoder := json.NewEncoder(resultWriter)
	if err := encoder.Encode(result); err != nil {
		ctx.Log.Error("Error encoding details.json", "err", err)
		return err
	}

	// Send uncompressed logs.
	logsBuffer := ctx.LogBuffer()
	if logsBuffer != nil {
		logsWriter, err := multipartWriter.CreateFormFile("file", "logs.txt")
		if err != nil {
			ctx.Log.Error("Error creating logs.txt", "err", err)
			return err
		}
		if _, err = logsWriter.Write(logsBuffer); err != nil {
			ctx.Log.Error("Error sending logs.txt", "err", err)
		}
	}

	// Send uncompressed tracing data.
	traceBuffer := ctx.TraceBuffer()
	if traceBuffer != nil {
		tracingWriter, err := multipartWriter.CreateFormFile("file", "tracing.json")
		if err != nil {
			ctx.Log.Error("Error creating tracing.json", "err", err)
			return err
		}
		if _, err = tracingWriter.Write(traceBuffer); err != nil {
			ctx.Log.Error("Error sending tracing.json", "err", err)
			return err
		}
	}

	return nil
}

func gradeRun(
	ctx *common.Context,
	client *http.Client,
	run *common.Run,
	multipartWriter *multipart.Writer,
) (*runner.RunResult, error) {
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent(
		"grade",
		common.EventBegin,
		common.Arg{Name: "id", Value: run.AttemptID},
		common.Arg{Name: "input", Value: run.InputHash},
		common.Arg{Name: "language", Value: run.Language},
	))
	defer func() {
		ctx.EventCollector.Add(ctx.EventFactory.NewEvent(
			"grade",
			common.EventEnd,
		))
	}()

	// Make sure no other I/O is being made while we grade this run.
	ioLockEvent := ctx.EventFactory.NewCompleteEvent("I/O lock")
	ioLock.Lock()
	defer ioLock.Unlock()
	ctx.EventCollector.Add(ioLockEvent)

	inputEvent := ctx.EventFactory.NewCompleteEvent("input")
	baseURL, err := url.Parse(ctx.Config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}
	inputRef, err := inputManager.Add(
		run.InputHash,
		runner.NewInputFactory(client, &ctx.Config, baseURL, run.ProblemName),
	)
	if err != nil {
		return nil, err
	}
	defer inputRef.Release()
	ctx.EventCollector.Add(inputEvent)

	// Send the header as soon as possible to avoid a timeout.
	filesWriter, err := multipartWriter.CreateFormFile("file", "files.zip")
	if err != nil {
		return nil, err
	}

	return runner.Grade(ctx, filesWriter, run, inputRef.Input, sandbox)
}
