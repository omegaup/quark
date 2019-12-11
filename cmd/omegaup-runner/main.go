package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"github.com/pkg/errors"
	"golang.org/x/net/http2"
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
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// One-shot mode: Performs a single operation and exits.
	oneshot = flag.String("oneshot", "",
		"Perform one action and return. Valid values are 'benchmark' and 'run'.")
	verbose = flag.Bool("verbose", false, "Enable verbose logging in oneshot mode.")
	request = flag.String("request", "",
		"With -oneshot=run, the path to the JSON request.")
	input = flag.String("input", "",
		"With -oneshot=run, the path to the input directory.")
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

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	flag.Parse()

	if *version {
		fmt.Printf("omegaup-runner %s\n", ProgramVersion)
		return
	}

	if *noop {
		sandbox = &runner.NoopSandbox{}
	} else {
		sandbox = &runner.OmegajailSandbox{}
	}

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := globalContext.Load().(*common.Context)
	if isOneShotMode() {
		tmpdir, err := ioutil.TempDir("", "quark-runner-oneshot")
		if err != nil {
			panic(err)
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
			results, err := runner.RunHostBenchmark(
				ctx,
				inputManager,
				sandbox,
				&ioLock,
			)
			if err != nil {
				ctx.Log.Error("Failed to run benchmark", "err", err)
			} else {
				encoder := json.NewEncoder(os.Stdout)
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(results); err != nil {
					ctx.Log.Error("Failed to encode JSON", "err", err)
				}
			}
		} else if *oneshot == "run" {
			if *request == "" {
				ctx.Log.Error("Missing -request parameter")
				return
			}
			if *input == "" {
				ctx.Log.Error("Missing -input parameter")
				return
			}
			f, err := os.Open(*request)
			if err != nil {
				ctx.Log.Error("Error opening request", "err", err)
				return
			}
			defer f.Close()

			var run common.Run
			if err := json.NewDecoder(f).Decode(&run); err != nil {
				ctx.Log.Error("Error reading request", "err", err)
				return
			}
			if *debug {
				run.Debug = true
			}

			inputRef, err := inputManager.Add(
				run.InputHash,
				runner.NewCachedInputFactory(*input),
			)
			if err != nil {
				ctx.Log.Error("Error loading input", "hash", run.InputHash, "err", err)
				return
			}
			defer inputRef.Release()

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
